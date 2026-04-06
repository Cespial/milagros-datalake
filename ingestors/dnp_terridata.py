"""DNP socioeconomic indicators via datos.gov.co (Medición del Desempeño Municipal).

Dataset: DNP - Medición del Desempeño Municipal (MDM)
datos.gov.co dataset ID: nkjx-rsq7
Endpoint: https://www.datos.gov.co/resource/nkjx-rsq7.json

The original TerriData REST API at terridata.dnp.gov.co/api/indicadores/municipio/{code}
returned 404 for all municipalities. The MDM dataset on datos.gov.co provides
the official DNP municipal performance metrics covering all AOI municipalities.

Fetches all MDM records for AOI municipalities. Saves one JSON file per municipality.
"""

import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_MUNICIPIOS
from ingestors.base import BaseIngestor

log = structlog.get_logger()

# datos.gov.co Socrata API — DNP Medición del Desempeño Municipal
DATOS_GOV_MDM_URL = "https://www.datos.gov.co/resource/nkjx-rsq7.json"
PAGE_SIZE = 5000


class DnpTerridataIngestor(BaseIngestor):
    name = "dnp_terridata"
    source_type = "api"
    data_type = "tabular"
    category = "socioeconomico"
    schedule = "monthly"
    license = "CC0"

    def fetch(self, **kwargs) -> list[Path]:
        paths: list[Path] = []

        for code, municipio_name in AOI_MUNICIPIOS.items():
            out_path = self.bronze_dir / f"terridata_{code}.json"
            if out_path.exists():
                log.info(
                    "dnp_terridata.skip_existing",
                    code=code,
                    municipio=municipio_name,
                    path=str(out_path),
                )
                paths.append(out_path)
                continue

            records = self._fetch_municipio(code, municipio_name)
            if records is not None:
                payload = {
                    "dane_code": code,
                    "municipio": municipio_name,
                    "source": "DNP Medición del Desempeño Municipal (datos.gov.co/d/nkjx-rsq7)",
                    "data": records,
                }
                out_path.write_text(
                    json.dumps(payload, indent=2, ensure_ascii=False),
                    encoding="utf-8",
                )
                log.info(
                    "dnp_terridata.saved",
                    code=code,
                    municipio=municipio_name,
                    records=len(records),
                    path=str(out_path),
                )
                paths.append(out_path)

        return paths

    def _fetch_municipio(
        self, code: str, municipio_name: str
    ) -> list[dict] | None:
        """Fetch MDM records for a municipality from datos.gov.co Socrata API."""
        log.info(
            "dnp_terridata.fetch",
            code=code,
            municipio=municipio_name,
        )

        all_records: list[dict] = []
        offset = 0

        try:
            while True:
                params = {
                    "$where": f"codigo_entidad='{code}'",
                    "$limit": PAGE_SIZE,
                    "$offset": offset,
                    "$order": "anio ASC",
                }
                resp = httpx.get(
                    DATOS_GOV_MDM_URL,
                    params=params,
                    timeout=60,
                    follow_redirects=True,
                )
                resp.raise_for_status()
                batch = resp.json()
                all_records.extend(batch)

                if len(batch) < PAGE_SIZE:
                    break
                offset += PAGE_SIZE

        except httpx.HTTPStatusError as exc:
            log.warning(
                "dnp_terridata.http_error",
                code=code,
                status=exc.response.status_code,
                msg="datos.gov.co API error — skipping",
            )
            return None
        except Exception as exc:
            log.warning(
                "dnp_terridata.fetch_failed",
                code=code,
                municipio=municipio_name,
                error=str(exc),
                msg="datos.gov.co unavailable — skipping",
            )
            return None

        log.info(
            "dnp_terridata.fetched",
            code=code,
            municipio=municipio_name,
            total_records=len(all_records),
        )
        return all_records

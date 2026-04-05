"""DNP TerriData socioeconomic indicators via public API.

Endpoint: terridata.dnp.gov.co/api/indicadores/municipio/{code}

Fetches all available indicators per AOI municipality.
Saves one JSON file per municipality: terridata_{code}.json.
Gracefully falls back (empty result) if API is unavailable.
"""

import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_MUNICIPIOS
from ingestors.base import BaseIngestor

log = structlog.get_logger()

TERRIDATA_URL = "https://terridata.dnp.gov.co/api/indicadores/municipio"


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

            data = self._fetch_municipio(code, municipio_name)
            if data is not None:
                payload = {
                    "dane_code": code,
                    "municipio": municipio_name,
                    "data": data,
                }
                out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
                log.info(
                    "dnp_terridata.saved",
                    code=code,
                    municipio=municipio_name,
                    path=str(out_path),
                )
                paths.append(out_path)

        return paths

    def _fetch_municipio(self, code: str, municipio_name: str) -> list | dict | None:
        """Fetch TerriData indicators for a municipality. Returns None on failure."""
        url = f"{TERRIDATA_URL}/{code}"
        log.info("dnp_terridata.fetch", code=code, municipio=municipio_name, url=url)

        try:
            resp = httpx.get(url, timeout=60, follow_redirects=True)
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as exc:
            log.warning(
                "dnp_terridata.http_error",
                code=code,
                status=exc.response.status_code,
                msg="API unavailable or municipality not found — skipping",
            )
        except Exception as exc:
            log.warning(
                "dnp_terridata.fetch_failed",
                code=code,
                municipio=municipio_name,
                error=str(exc),
                msg="TerriData API unavailable — skipping",
            )

        return None

"""AGRONET EVA (Evaluaciones Agropecuarias) agricultural data via datos.gov.co.

Dataset: uejq-wxrr — Producción agrícola por municipio (área, producción, rendimiento)

Queries per municipality code from AOI_MUNICIPIOS.
Saves results as a single JSON file: eva_agricola.json.
"""

import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_MUNICIPIOS
from ingestors.base import BaseIngestor

log = structlog.get_logger()

BASE_URL = "https://www.datos.gov.co/resource"
DATASET_ID = "uejq-wxrr"
PAGE_SIZE = 50_000

# Common column names for municipality code in EVA dataset
MPIO_COLUMNS = [
    "cod_mpio", "codigo_municipio", "cod_municipio",
    "divipola", "codigo_divipola", "municipio_codigo",
    "codmpio",
]


class AgronetEvaIngestor(BaseIngestor):
    name = "agronet_eva"
    source_type = "api"
    data_type = "tabular"
    category = "socioeconomico"
    schedule = "monthly"
    license = "CC0"

    def fetch(self, **kwargs) -> list[Path]:
        out_path = self.bronze_dir / "eva_agricola.json"
        if out_path.exists():
            log.info("agronet_eva.skip_existing", path=str(out_path))
            return [out_path]

        url = f"{BASE_URL}/{DATASET_ID}.json"
        mpio_col = self._detect_mpio_column(url)
        log.info("agronet_eva.mpio_column", column=mpio_col)

        all_records: list[dict] = []

        for code, municipio_name in AOI_MUNICIPIOS.items():
            records = self._fetch_municipio(url, code, municipio_name, mpio_col)
            all_records.extend(records)

        out_path.write_text(json.dumps(all_records, indent=2, ensure_ascii=False))
        log.info("agronet_eva.saved", records=len(all_records), path=str(out_path))
        return [out_path]

    def _fetch_municipio(
        self, url: str, code: str, municipio_name: str, mpio_col: str | None
    ) -> list[dict]:
        records: list[dict] = []
        offset = 0

        while True:
            params: dict = {"$limit": PAGE_SIZE, "$offset": offset}
            if mpio_col:
                params["$where"] = f"{mpio_col} = '{code}'"

            log.info("agronet_eva.fetch_page", code=code, municipio=municipio_name, offset=offset)

            try:
                resp = httpx.get(url, params=params, timeout=120)
                resp.raise_for_status()
                page = resp.json()
            except Exception as exc:
                log.error("agronet_eva.page_failed", code=code, error=str(exc))
                break

            if not page:
                break

            records.extend(page)
            log.info(
                "agronet_eva.page_done",
                code=code,
                offset=offset,
                page_size=len(page),
                total=len(records),
            )

            if len(page) < PAGE_SIZE:
                break
            offset += PAGE_SIZE

        return records

    def _detect_mpio_column(self, url: str) -> str | None:
        """Probe dataset to detect municipality code column."""
        try:
            resp = httpx.get(url, params={"$limit": 1}, timeout=30)
            resp.raise_for_status()
            sample = resp.json()
            if sample and isinstance(sample, list):
                first = sample[0]
                for col in MPIO_COLUMNS:
                    if col in first:
                        return col
        except Exception as exc:
            log.debug("agronet_eva.probe_failed", error=str(exc))
        return None

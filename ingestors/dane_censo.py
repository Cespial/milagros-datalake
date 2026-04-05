"""DANE census data via datos.gov.co SODA API.

Datasets:
  - poblacion_2018   (qtp5-v59k) — 2018 census population by municipality
  - proyecciones     (nlxm-gsci) — population projections 2018-2035
  - viviendas        (sn8c-bwqk) — housing characteristics 2018

Queries per municipality code (DANE 5-digit). Saves one JSON per dataset.
"""

import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_MUNICIPIOS
from ingestors.base import BaseIngestor

log = structlog.get_logger()

BASE_URL = "https://www.datos.gov.co/resource"

DATASETS = {
    "poblacion_2018": "qtp5-v59k",
    "proyecciones":   "nlxm-gsci",
    "viviendas":      "sn8c-bwqk",
}

PAGE_SIZE = 50_000

# Common column names used by DANE datasets for municipality code
MPIO_COLUMNS = [
    "cod_mpio", "codigo_municipio", "cod_municipio",
    "divipola", "codigo_divipola", "mpio_cdpmp",
]


class DaneCensoIngestor(BaseIngestor):
    name = "dane_censo"
    source_type = "api"
    data_type = "tabular"
    category = "socioeconomico"
    schedule = "once"
    license = "CC0"

    def fetch(self, **kwargs) -> list[Path]:
        paths: list[Path] = []

        for dataset_name, dataset_id in DATASETS.items():
            out_path = self.bronze_dir / f"{dataset_name}.json"
            if out_path.exists():
                log.info("dane_censo.skip_existing", dataset=dataset_name, path=str(out_path))
                paths.append(out_path)
                continue

            records = self._fetch_dataset(dataset_name, dataset_id)
            if records:
                out_path.write_text(json.dumps(records, indent=2, ensure_ascii=False))
                log.info("dane_censo.saved", dataset=dataset_name, records=len(records), path=str(out_path))
                paths.append(out_path)

        return paths

    def _fetch_dataset(self, dataset_name: str, dataset_id: str) -> list[dict]:
        """Fetch all AOI municipalities from a DANE dataset."""
        url = f"{BASE_URL}/{dataset_id}.json"
        all_records: list[dict] = []

        # First probe the dataset to find the municipality column
        mpio_col = self._detect_mpio_column(url, dataset_id)
        log.info("dane_censo.mpio_column", dataset=dataset_name, column=mpio_col)

        codes = list(AOI_MUNICIPIOS.keys())

        for code in codes:
            offset = 0
            while True:
                params: dict = {
                    "$limit": PAGE_SIZE,
                    "$offset": offset,
                }
                if mpio_col:
                    params["$where"] = f"{mpio_col} = '{code}'"

                log.info(
                    "dane_censo.fetch_page",
                    dataset=dataset_name,
                    code=code,
                    offset=offset,
                )
                try:
                    resp = httpx.get(url, params=params, timeout=120)
                    resp.raise_for_status()
                    page = resp.json()
                except Exception as exc:
                    log.error(
                        "dane_censo.page_failed",
                        dataset=dataset_name,
                        code=code,
                        error=str(exc),
                    )
                    break

                if not page:
                    break

                all_records.extend(page)

                if len(page) < PAGE_SIZE:
                    break
                offset += PAGE_SIZE

        return all_records

    def _detect_mpio_column(self, url: str, dataset_id: str) -> str | None:
        """Probe a single record to detect the municipality code column name."""
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
            log.debug("dane_censo.probe_failed", dataset=dataset_id, error=str(exc))
        return None

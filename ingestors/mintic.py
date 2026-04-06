"""MinTIC telecommunications coverage ingestor via datos.gov.co.

Datasets tried (MinTIC mobile coverage data):
  - `9mey-c8s8`  — Cobertura móvil por municipio (primary)
  - `xkxg-a4mx`  — Conectividad móvil nacional
  - `tqsy-m7ba`  — Cobertura 4G/5G por municipio

Filters records by DANE municipality codes from AOI_MUNICIPIOS.
Saves one JSON file per dataset that returns data.
"""

import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_MUNICIPIOS
from ingestors.base import BaseIngestor

log = structlog.get_logger()

BASE_URL = "https://www.datos.gov.co/resource"

# MinTIC coverage datasets — confirmed 2026-04
# 9mey-c8s8: cobertura movil por tecnologia, depto y municipio
CANDIDATE_DATASETS = {
    "cobertura_movil": ["9mey-c8s8", "v8qi-smj7", "xkxg-a4mx"],
    "cobertura_banda_ancha": ["tqsy-m7ba", "qqys-h2mr", "vf7d-nk8j"],
}

PAGE_SIZE = 50_000

# Possible column names for municipality code in MinTIC datasets
MPIO_COLUMNS = [
    "cod_municipio", "codigo_municipio", "codmpio",
    "divipola", "codigo_divipola", "cod_mpio",
    "municipio_codigo", "id_municipio",
]


class MinticIngestor(BaseIngestor):
    name = "mintic"
    source_type = "api"
    data_type = "tabular"
    category = "infraestructura"
    schedule = "quarterly"
    license = "Datos Abiertos Colombia (MinTIC)"

    def fetch(self, **kwargs) -> list[Path]:
        paths: list[Path] = []

        for dataset_name, candidate_ids in CANDIDATE_DATASETS.items():
            out_path = self.bronze_dir / f"{dataset_name}.json"
            if out_path.exists():
                log.info("mintic.skip_existing", dataset=dataset_name)
                paths.append(out_path)
                continue

            records = self._fetch_dataset(dataset_name, candidate_ids)
            if records:
                out_path.write_text(json.dumps(records, indent=2, ensure_ascii=False))
                log.info("mintic.saved", dataset=dataset_name, records=len(records), path=str(out_path))
                paths.append(out_path)
            else:
                log.warning("mintic.no_data", dataset=dataset_name)

        return paths

    def _fetch_dataset(self, dataset_name: str, candidate_ids: list[str]) -> list[dict]:
        """Try each dataset ID until one returns data."""
        for dataset_id in candidate_ids:
            url = f"{BASE_URL}/{dataset_id}.json"
            log.info("mintic.try_dataset", name=dataset_name, id=dataset_id)

            # Probe to find municipality column
            mpio_col = self._detect_mpio_column(url, dataset_id)
            log.info("mintic.mpio_column", dataset=dataset_name, id=dataset_id, column=mpio_col)

            records = self._paginate(url, dataset_id, mpio_col)
            if records:
                log.info("mintic.dataset_ok", id=dataset_id, records=len(records))
                return records

            log.debug("mintic.dataset_empty", id=dataset_id)

        return []

    def _paginate(self, url: str, dataset_id: str, mpio_col: str | None) -> list[dict]:
        """Fetch all records matching AOI municipalities."""
        all_records: list[dict] = []
        codes = list(AOI_MUNICIPIOS.keys())

        if mpio_col:
            # Filter by each municipality code
            for code in codes:
                offset = 0
                while True:
                    params: dict = {
                        "$limit": PAGE_SIZE,
                        "$offset": offset,
                        "$where": f"{mpio_col} = '{code}'",
                    }
                    try:
                        resp = httpx.get(url, params=params, timeout=120)
                        resp.raise_for_status()
                        page = resp.json()
                    except Exception as exc:
                        log.warning("mintic.page_failed", id=dataset_id, code=code, error=str(exc))
                        break

                    if not page:
                        break
                    all_records.extend(page)
                    if len(page) < PAGE_SIZE:
                        break
                    offset += PAGE_SIZE
        else:
            # No filter column found — download all records
            log.warning("mintic.no_filter", id=dataset_id, msg="downloading all records")
            offset = 0
            while True:
                params = {"$limit": PAGE_SIZE, "$offset": offset}
                try:
                    resp = httpx.get(url, params=params, timeout=120)
                    resp.raise_for_status()
                    page = resp.json()
                except Exception as exc:
                    log.warning("mintic.page_failed", id=dataset_id, error=str(exc))
                    break

                if not page:
                    break
                all_records.extend(page)
                if len(page) < PAGE_SIZE:
                    break
                offset += PAGE_SIZE

        return all_records

    def _detect_mpio_column(self, url: str, dataset_id: str) -> str | None:
        """Probe the first record to find the municipality code column."""
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
            log.debug("mintic.probe_failed", id=dataset_id, error=str(exc))
        return None

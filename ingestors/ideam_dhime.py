"""IDEAM DHIME hydrology station data via datos.gov.co SODA API.

Datasets:
  - Nivel instantaneo del rio: bdmn-sqnh (proxy for discharge, 2001-now)
  - Nivel maximo del rio: vfth-yucv (daily max, 2001-now)
  - Nivel minimo del rio: pt9a-aamx (daily min, 2001-now)
  - Precipitacion: s54a-sgyg (2003-now)

Note: IDEAM removed the caudales (us7c-gwhb) dataset from datos.gov.co.
River level is the standard proxy for discharge in Colombian hydrology.

Paginated with $limit/$offset. Filtered by AOI bounding box.
"""

import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_BBOX, AOI_MUNICIPIOS
from ingestors.base import BaseIngestor

log = structlog.get_logger()

BASE_URL = "https://www.datos.gov.co/resource"

DATASETS = {
    "nivel_instantaneo": "bdmn-sqnh",
    "nivel_maximo": "vfth-yucv",
    "nivel_minimo": "pt9a-aamx",
    "precipitacion": "s54a-sgyg",
}

PAGE_SIZE = 50_000


class IdeamDhimeIngestor(BaseIngestor):
    name = "ideam_dhime"
    source_type = "api"
    data_type = "tabular"
    category = "hidrologia"
    schedule = "monthly"
    license = "CC0"

    def fetch(self, **kwargs) -> list[Path]:
        paths: list[Path] = []

        for variable, dataset_id in DATASETS.items():
            out_path = self.bronze_dir / f"{variable}.json"
            if out_path.exists():
                log.info("ideam_dhime.skip_existing", variable=variable, path=str(out_path))
                paths.append(out_path)
                continue

            records = self._paginate(dataset_id, variable)
            out_path.write_text(json.dumps(records, indent=2, ensure_ascii=False))
            log.info("ideam_dhime.saved", variable=variable, records=len(records), path=str(out_path))
            paths.append(out_path)

        return paths

    def _paginate(self, dataset_id: str, variable: str) -> list[dict]:
        """Paginate through datos.gov.co SODA API with bbox filter."""
        west = AOI_BBOX["west"]
        east = AOI_BBOX["east"]
        south = AOI_BBOX["south"]
        north = AOI_BBOX["north"]

        # Try common column names for lat/lon in IDEAM datasets
        lat_col = "latitud"
        lon_col = "longitud"

        all_records: list[dict] = []
        offset = 0

        while True:
            url = f"{BASE_URL}/{dataset_id}.json"
            params = {
                "$limit": PAGE_SIZE,
                "$offset": offset,
                "$where": (
                    f"{lat_col} >= '{south}' AND {lat_col} <= '{north}' "
                    f"AND {lon_col} >= '{west}' AND {lon_col} <= '{east}'"
                ),
            }
            log.info("ideam_dhime.fetch_page", dataset=dataset_id, variable=variable, offset=offset)

            try:
                resp = httpx.get(url, params=params, timeout=120)
                resp.raise_for_status()
                page = resp.json()
            except Exception as exc:
                # Fallback: fetch without bbox filter if columns differ
                log.warning(
                    "ideam_dhime.bbox_filter_failed",
                    dataset=dataset_id,
                    error=str(exc),
                    msg="retrying without spatial filter",
                )
                fallback_params = {"$limit": PAGE_SIZE, "$offset": offset}
                resp = httpx.get(url, params=fallback_params, timeout=120)
                resp.raise_for_status()
                page = resp.json()

            if not page:
                break

            all_records.extend(page)
            log.info(
                "ideam_dhime.page_done",
                variable=variable,
                offset=offset,
                page_size=len(page),
                total=len(all_records),
            )

            if len(page) < PAGE_SIZE:
                break
            offset += PAGE_SIZE

        return all_records

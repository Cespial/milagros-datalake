"""UPME generation project registry via datos.gov.co.

Dataset: gknd-ij62 — Registro de proyectos de generación eléctrica (UPME)

Filtered by departamento='ANTIOQUIA' to capture projects relevant to the AOI.
Saves as: proyectos_generacion.json
"""

import json
from pathlib import Path

import httpx
import structlog

from ingestors.base import BaseIngestor

log = structlog.get_logger()

BASE_URL = "https://www.datos.gov.co/resource"
DATASET_ID = "gknd-ij62"
PAGE_SIZE = 50_000

# Department filter variations (the dataset may use different spellings)
DEPT_FILTERS = [
    "departamento = 'ANTIOQUIA'",
    "departamento = 'Antioquia'",
    "dpto = 'ANTIOQUIA'",
    "depto = 'ANTIOQUIA'",
]


class UpmeProyectosIngestor(BaseIngestor):
    name = "upme_proyectos"
    source_type = "api"
    data_type = "tabular"
    category = "mercado_electrico"
    schedule = "monthly"
    license = "CC0"

    def fetch(self, **kwargs) -> list[Path]:
        out_path = self.bronze_dir / "proyectos_generacion.json"
        if out_path.exists():
            log.info("upme_proyectos.skip_existing", path=str(out_path))
            return [out_path]

        url = f"{BASE_URL}/{DATASET_ID}.json"
        records = self._fetch_with_filter(url)

        out_path.write_text(json.dumps(records, indent=2, ensure_ascii=False))
        log.info("upme_proyectos.saved", records=len(records), path=str(out_path))
        return [out_path]

    def _fetch_with_filter(self, url: str) -> list[dict]:
        """Try successive department filter expressions; fall back to unfiltered."""
        # Try each filter variation
        for where_clause in DEPT_FILTERS:
            records = self._paginate(url, where_clause=where_clause)
            if records:
                log.info("upme_proyectos.filter_ok", where=where_clause, records=len(records))
                return records
            log.debug("upme_proyectos.filter_empty", where=where_clause)

        # Fallback: fetch all records without filter
        log.warning(
            "upme_proyectos.no_filter_match",
            msg="department filter returned empty — fetching all records",
        )
        return self._paginate(url, where_clause=None)

    def _paginate(self, url: str, where_clause: str | None) -> list[dict]:
        all_records: list[dict] = []
        offset = 0

        while True:
            params: dict = {"$limit": PAGE_SIZE, "$offset": offset}
            if where_clause:
                params["$where"] = where_clause

            log.info("upme_proyectos.fetch_page", offset=offset, where=where_clause)

            try:
                resp = httpx.get(url, params=params, timeout=120)
                resp.raise_for_status()
                page = resp.json()
            except Exception as exc:
                log.error("upme_proyectos.page_failed", offset=offset, error=str(exc))
                break

            if not page:
                break

            all_records.extend(page)
            log.info(
                "upme_proyectos.page_done",
                offset=offset,
                page_size=len(page),
                total=len(all_records),
            )

            if len(page) < PAGE_SIZE:
                break
            offset += PAGE_SIZE

        return all_records

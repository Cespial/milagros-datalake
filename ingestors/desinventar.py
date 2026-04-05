"""DesInventar disaster inventory for Antioquia/AOI.

Primary source:
  DesInventar REST API — desinventar.org/api/colombia/

Fallback source:
  datos.gov.co dataset he96-kbic — filtered by departamento='ANTIOQUIA'

Saves: desinventar_desastres.json
"""

import json
from pathlib import Path

import httpx
import structlog

from ingestors.base import BaseIngestor

log = structlog.get_logger()

# DesInventar public API for Colombia
DESINVENTAR_API = "https://www.desinventar.net/DesInventar/api/colombia/disasters"

# datos.gov.co fallback
DATOS_BASE = "https://www.datos.gov.co/resource"
DATOS_DATASET_ID = "he96-kbic"
PAGE_SIZE = 50_000

DEPT_FILTERS = [
    "departamento = 'ANTIOQUIA'",
    "departamento = 'Antioquia'",
    "depto = 'ANTIOQUIA'",
    "nombre_departamento = 'ANTIOQUIA'",
]


class DesinventarIngestor(BaseIngestor):
    name = "desinventar"
    source_type = "api"
    data_type = "tabular"
    category = "geologia"
    schedule = "monthly"
    license = "CC0"

    def fetch(self, **kwargs) -> list[Path]:
        out_path = self.bronze_dir / "desinventar_desastres.json"
        if out_path.exists():
            log.info("desinventar.skip_existing", path=str(out_path))
            return [out_path]

        # Try primary source first
        records = self._fetch_desinventar_api()

        if not records:
            log.warning("desinventar.primary_empty", msg="Trying datos.gov.co fallback")
            records = self._fetch_datos_gov()

        if not records:
            log.warning("desinventar.all_sources_empty", msg="No records retrieved from any source")

        out_path.write_text(json.dumps(records, indent=2, ensure_ascii=False))
        log.info("desinventar.saved", records=len(records), path=str(out_path))
        return [out_path]

    def _fetch_desinventar_api(self) -> list[dict]:
        """Try DesInventar REST API for Colombia, Antioquia filter."""
        params = {
            "departamento": "ANTIOQUIA",
            "format": "json",
        }
        log.info("desinventar.fetch_primary", url=DESINVENTAR_API)

        try:
            resp = httpx.get(DESINVENTAR_API, params=params, timeout=60, follow_redirects=True)
            resp.raise_for_status()
            data = resp.json()

            # API may return list or dict with records key
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                # Try common key names
                for key in ("disasters", "records", "data", "results", "items"):
                    if key in data and isinstance(data[key], list):
                        return data[key]
                # Return full dict wrapped in list as fallback
                return [data]

        except Exception as exc:
            log.warning("desinventar.primary_failed", error=str(exc))

        return []

    def _fetch_datos_gov(self) -> list[dict]:
        """Fallback: paginate datos.gov.co dataset filtered by Antioquia."""
        url = f"{DATOS_BASE}/{DATOS_DATASET_ID}.json"

        # Try filter variants
        for where_clause in DEPT_FILTERS:
            records = self._paginate(url, where_clause)
            if records:
                log.info("desinventar.datos_gov_ok", where=where_clause, records=len(records))
                return records
            log.debug("desinventar.datos_gov_empty_filter", where=where_clause)

        # Fallback: all records without filter
        log.warning("desinventar.datos_gov_no_filter_match", msg="fetching all records")
        return self._paginate(url, where_clause=None)

    def _paginate(self, url: str, where_clause: str | None) -> list[dict]:
        all_records: list[dict] = []
        offset = 0

        while True:
            params: dict = {"$limit": PAGE_SIZE, "$offset": offset}
            if where_clause:
                params["$where"] = where_clause

            log.info("desinventar.fetch_page", offset=offset, where=where_clause)

            try:
                resp = httpx.get(url, params=params, timeout=120)
                resp.raise_for_status()
                page = resp.json()
            except Exception as exc:
                log.error("desinventar.page_failed", offset=offset, error=str(exc))
                break

            if not page:
                break

            all_records.extend(page)
            log.info(
                "desinventar.page_done",
                offset=offset,
                page_size=len(page),
                total=len(all_records),
            )

            if len(page) < PAGE_SIZE:
                break
            offset += PAGE_SIZE

        return all_records

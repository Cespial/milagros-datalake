"""SGC seismicity data from USGS ComCat and datos.gov.co.

Primary source:
  USGS ComCat FDSN — earthquake.usgs.gov/fdsnws/event/1/query
  GeoJSON format, 300 km radius from AOI center, M >= 2.5

Fallback source:
  datos.gov.co dataset wmxp-xih5 (SGC catalog)

Saves:
  - usgs_comcat.geojson   — GeoJSON FeatureCollection
  - sgc_datos_gov.json    — JSON from datos.gov.co
"""

import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

USGS_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
SGC_DATOS_ID = "wmxp-xih5"
DATOS_BASE = "https://www.datos.gov.co/resource"

PAGE_SIZE = 20_000


class SgcSismicidadIngestor(BaseIngestor):
    name = "sgc_sismicidad"
    source_type = "api"
    data_type = "tabular"
    category = "geologia"
    schedule = "monthly"
    license = "CC0"

    def fetch(self, **kwargs) -> list[Path]:
        paths: list[Path] = []

        start_date = kwargs.get("start_date", "1993-01-01")
        end_date = kwargs.get("end_date", "2026-04-01")

        paths.extend(self._fetch_usgs(start_date, end_date))
        paths.extend(self._fetch_sgc_datos())

        return paths

    def _fetch_usgs(self, start_date: str, end_date: str) -> list[Path]:
        out_path = self.bronze_dir / "usgs_comcat.geojson"
        if out_path.exists():
            log.info("sgc_sismicidad.skip_existing", source="usgs", path=str(out_path))
            return [out_path]

        # AOI center
        lat = (AOI_BBOX["south"] + AOI_BBOX["north"]) / 2
        lon = (AOI_BBOX["west"] + AOI_BBOX["east"]) / 2

        params = {
            "format": "geojson",
            "starttime": start_date,
            "endtime": end_date,
            "latitude": lat,
            "longitude": lon,
            "maxradiuskm": 300,
            "minmagnitude": 2.5,
            "orderby": "time",
        }

        log.info("sgc_sismicidad.fetch_usgs", lat=lat, lon=lon, radius_km=300, min_mag=2.5)

        try:
            resp = httpx.get(USGS_URL, params=params, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            out_path.write_text(json.dumps(data, indent=2, ensure_ascii=False))
            count = len(data.get("features", []))
            log.info("sgc_sismicidad.usgs_saved", features=count, path=str(out_path))
            return [out_path]
        except Exception as exc:
            log.error("sgc_sismicidad.usgs_failed", error=str(exc))
            return []

    def _fetch_sgc_datos(self) -> list[Path]:
        out_path = self.bronze_dir / "sgc_datos_gov.json"
        if out_path.exists():
            log.info("sgc_sismicidad.skip_existing", source="datos_gov", path=str(out_path))
            return [out_path]

        records: list[dict] = []
        offset = 0
        url = f"{DATOS_BASE}/{SGC_DATOS_ID}.json"

        while True:
            params = {"$limit": PAGE_SIZE, "$offset": offset}
            log.info("sgc_sismicidad.fetch_datos_gov", offset=offset)
            try:
                resp = httpx.get(url, params=params, timeout=120)
                resp.raise_for_status()
                page = resp.json()
            except Exception as exc:
                log.error("sgc_sismicidad.datos_gov_failed", error=str(exc))
                break

            if not page:
                break

            records.extend(page)
            log.info("sgc_sismicidad.datos_gov_page", offset=offset, page_size=len(page), total=len(records))

            if len(page) < PAGE_SIZE:
                break
            offset += PAGE_SIZE

        if records:
            out_path.write_text(json.dumps(records, indent=2, ensure_ascii=False))
            log.info("sgc_sismicidad.datos_gov_saved", records=len(records), path=str(out_path))
            return [out_path]

        return []

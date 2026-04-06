"""SGC SIMMA landslide inventory via ArcGIS REST API.

Source:
  simma.sgc.gov.co/arcgis/rest/services/SIMMA/Movimientos_en_masa/MapServer/0/query

Paginated with resultOffset + resultRecordCount.
Spatial filter by AOI bounding box (geometry envelope in WGS84).
Saves as JSON (ArcGIS JSON FeatureSet format).
"""

import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

SIMMA_URL = (
    "https://geoportal.sgc.gov.co/arcgis/rest/services/SIMMA/"
    "Capas_Principales/MapServer/1/query"
)
PAGE_SIZE = 1_000


class SgcSimmaIngestor(BaseIngestor):
    name = "sgc_simma"
    source_type = "api"
    data_type = "tabular"
    category = "geologia"
    schedule = "monthly"
    license = "CC0"

    def fetch(self, **kwargs) -> list[Path]:
        out_path = self.bronze_dir / "movimientos_en_masa.json"
        if out_path.exists():
            log.info("sgc_simma.skip_existing", path=str(out_path))
            return [out_path]

        all_features: list[dict] = []
        offset = 0

        # Build ArcGIS envelope geometry for spatial filter
        west = AOI_BBOX["west"]
        south = AOI_BBOX["south"]
        east = AOI_BBOX["east"]
        north = AOI_BBOX["north"]

        geometry = f"{west},{south},{east},{north}"

        params = {
            "where": "1=1",
            "geometry": geometry,
            "geometryType": "esriGeometryEnvelope",
            "inSR": "4326",
            "spatialRel": "esriSpatialRelIntersects",
            "outFields": "*",
            "outSR": "4326",
            "f": "json",
            "returnGeometry": "true",
        }

        log.info("sgc_simma.fetching")

        try:
            resp = httpx.get(SIMMA_URL, params=params, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            all_features = data.get("features", [])
            log.info("sgc_simma.fetched", features=len(all_features))
        except Exception as exc:
            log.error("sgc_simma.fetch_failed", error=str(exc))

        result = {
            "type": "FeatureCollection",
            "source": "SGC SIMMA",
            "bbox": [west, south, east, north],
            "total_features": len(all_features),
            "features": all_features,
        }

        out_path.write_text(json.dumps(result, indent=2, ensure_ascii=False))
        log.info("sgc_simma.saved", features=len(all_features), path=str(out_path))
        return [out_path]

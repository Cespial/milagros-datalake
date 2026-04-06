"""RUNAP (Registro Único Nacional de Áreas Protegidas) ingestor via ArcGIS REST.

Source: PNN Colombia ArcGIS REST service
Downloads protected area polygons with spatial filter by AOI_BBOX.
Outputs a single GeoJSON with all intersecting areas.
"""

import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

# PNN (Parques Nacionales Naturales de Colombia) ArcGIS REST endpoint
BASE_URL = (
    "https://mapas.parquesnacionales.gov.co/arcgis/rest/services"
    "/pnn/runap/FeatureServer/0/query"
)

PAGE_SIZE = 5000


class RunapIngestor(BaseIngestor):
    name = "runap"
    source_type = "arcgis_rest"
    data_type = "vector"
    category = "biodiversidad"
    schedule = "annual"
    license = "Datos Abiertos Colombia (PNN)"

    def fetch(self, **kwargs) -> list[Path]:
        out_path = self.bronze_dir / "runap.geojson"
        if out_path.exists():
            log.info("runap.skip_existing")
            return [out_path]

        bbox = (
            f"{AOI_BBOX['west']},{AOI_BBOX['south']}"
            f",{AOI_BBOX['east']},{AOI_BBOX['north']}"
        )

        params = {
            "where": "1=1",
            "geometry": bbox,
            "geometryType": "esriGeometryEnvelope",
            "spatialRel": "esriSpatialRelIntersects",
            "inSR": "4326",
            "outFields": "*",
            "returnGeometry": "true",
            "outSR": "4326",
            "f": "geojson",
            "resultRecordCount": PAGE_SIZE,
        }

        all_features = []
        offset = 0

        while True:
            params["resultOffset"] = offset
            log.info("runap.fetching", offset=offset)

            response = httpx.get(BASE_URL, params=params, timeout=120)
            response.raise_for_status()
            data = response.json()

            features = data.get("features", [])
            all_features.extend(features)

            if len(features) < PAGE_SIZE:
                break

            offset += PAGE_SIZE

        geojson = {
            "type": "FeatureCollection",
            "features": all_features,
        }
        out_path.write_text(json.dumps(geojson, ensure_ascii=False), encoding="utf-8")
        log.info(
            "runap.saved",
            path=str(out_path),
            features=len(all_features),
        )
        return [out_path]

"""Corine Land Cover Colombia ingestor via ArcGIS REST FeatureServer.

Downloads land cover polygons for 5 epochs (2000, 2002, 2008, 2012, 2018)
with spatial filter by AOI_BBOX. Outputs one GeoJSON per epoch.
"""

import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

# IDEAM / SIAC ArcGIS REST endpoint for Corine Land Cover Colombia
BASE_URL = (
    "https://gis.siac.gov.co/arcgis/rest/services"
    "/IDEAM/Corine_Land_Cover/FeatureServer"
)

# Layer index per epoch (confirmed order from SIAC service)
EPOCHS = {
    2000: 0,
    2002: 1,
    2008: 2,
    2012: 3,
    2018: 4,
}

PAGE_SIZE = 5000


class CorineLcIngestor(BaseIngestor):
    name = "corine_lc"
    source_type = "arcgis_rest"
    data_type = "vector"
    category = "biodiversidad"
    schedule = "once"
    license = "Datos Abiertos Colombia (IDEAM)"

    def _fetch_epoch(self, epoch: int, layer_id: int) -> list[dict]:
        """Paginate through all features for a given epoch layer."""
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

        url = f"{BASE_URL}/{layer_id}/query"
        all_features = []
        offset = 0

        while True:
            params["resultOffset"] = offset
            log.info("corine_lc.fetching", epoch=epoch, offset=offset)

            response = httpx.get(url, params=params, timeout=120)
            response.raise_for_status()
            data = response.json()

            features = data.get("features", [])
            all_features.extend(features)

            if len(features) < PAGE_SIZE:
                break

            offset += PAGE_SIZE

        log.info(
            "corine_lc.epoch_complete",
            epoch=epoch,
            total_features=len(all_features),
        )
        return all_features

    def fetch(self, **kwargs) -> list[Path]:
        paths = []

        for epoch, layer_id in EPOCHS.items():
            out_path = self.bronze_dir / f"corine_lc_{epoch}.geojson"
            if out_path.exists():
                log.info("corine_lc.skip_existing", epoch=epoch)
                paths.append(out_path)
                continue

            features = self._fetch_epoch(epoch, layer_id)

            geojson = {
                "type": "FeatureCollection",
                "features": features,
            }
            out_path.write_text(json.dumps(geojson, ensure_ascii=False), encoding="utf-8")
            log.info(
                "corine_lc.saved",
                epoch=epoch,
                path=str(out_path),
                features=len(features),
            )
            paths.append(out_path)

        return paths

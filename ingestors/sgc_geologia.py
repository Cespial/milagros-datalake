"""SGC geological map ingestor via ArcGIS REST.

Source: srvags.sgc.gov.co/arcgis/rest/services/Geologia/Mapa_geologico_Colombia_2020/MapServer
Layers:
  0 — unidades_geologicas
  1 — fallas

Paginates through results with spatial filter by AOI_BBOX. Outputs one GeoJSON per layer.
"""

import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

BASE_URL = (
    "https://geoportal.sgc.gov.co/arcgis/rest/services"
    "/Mapa_Geologico_Colombia/Mapa_Geologico_Colombia_V2023/MapServer"
)

LAYERS = {
    "unidades_geologicas": 733,
    "fallas": 704,
}

PAGE_SIZE = 5000


class SgcGeologiaIngestor(BaseIngestor):
    name = "sgc_geologia"
    source_type = "arcgis_rest"
    data_type = "vector"
    category = "geologia"
    schedule = "once"
    license = "Datos Abiertos Colombia"

    def _fetch_layer(self, layer_name: str, layer_id: int) -> list[dict]:
        """Paginate through all features for a given layer ID."""
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
            log.info(
                "sgc_geologia.fetching",
                layer=layer_name,
                offset=offset,
            )
            response = httpx.get(url, params=params, timeout=120)
            response.raise_for_status()
            data = response.json()

            features = data.get("features", [])
            all_features.extend(features)

            # Stop if fewer features returned than page size
            if len(features) < PAGE_SIZE:
                break

            offset += PAGE_SIZE

        log.info(
            "sgc_geologia.layer_complete",
            layer=layer_name,
            total_features=len(all_features),
        )
        return all_features

    def fetch(self, **kwargs) -> list[Path]:
        paths = []

        for layer_name, layer_id in LAYERS.items():
            out_path = self.bronze_dir / f"{layer_name}.geojson"
            if out_path.exists():
                log.info("sgc_geologia.skip_existing", layer=layer_name)
                paths.append(out_path)
                continue

            features = self._fetch_layer(layer_name, layer_id)

            geojson = {
                "type": "FeatureCollection",
                "features": features,
            }
            out_path.write_text(json.dumps(geojson, ensure_ascii=False), encoding="utf-8")
            log.info(
                "sgc_geologia.saved",
                layer=layer_name,
                path=str(out_path),
                features=len(features),
            )
            paths.append(out_path)

        return paths

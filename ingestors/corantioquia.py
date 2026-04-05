"""CORANTIOQUIA boundaries ingestor via ArcGIS REST.

Source: sig.corantioquia.gov.co
Layers:
  - jurisdiccion — CORANTIOQUIA jurisdiction boundary
  - POMCA        — Planes de Ordenación y Manejo de Cuencas Hidrográficas

Outputs one GeoJSON per layer.
"""

import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

BASE_URL = "https://sig.corantioquia.gov.co/arcgis/rest/services"

LAYERS = {
    "jurisdiccion": f"{BASE_URL}/Limites/Jurisdiccion_CORANTIOQUIA/MapServer/0/query",
    "POMCA": f"{BASE_URL}/Cuencas/POMCA/MapServer/0/query",
}

PAGE_SIZE = 5000


class CorantioquiaIngestor(BaseIngestor):
    name = "corantioquia"
    source_type = "arcgis_rest"
    data_type = "vector"
    category = "regulatorio"
    schedule = "once"
    license = "CORANTIOQUIA Datos Abiertos"

    def _fetch_layer(self, layer_name: str, url: str) -> list[dict]:
        """Paginate through all features for a given layer URL."""
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
            log.info("corantioquia.fetching", layer=layer_name, offset=offset)

            response = httpx.get(url, params=params, timeout=120)
            response.raise_for_status()
            data = response.json()

            features = data.get("features", [])
            all_features.extend(features)

            if len(features) < PAGE_SIZE:
                break

            offset += PAGE_SIZE

        log.info(
            "corantioquia.layer_complete",
            layer=layer_name,
            total_features=len(all_features),
        )
        return all_features

    def fetch(self, **kwargs) -> list[Path]:
        paths = []

        for layer_name, url in LAYERS.items():
            out_path = self.bronze_dir / f"{layer_name.lower()}.geojson"
            if out_path.exists():
                log.info("corantioquia.skip_existing", layer=layer_name)
                paths.append(out_path)
                continue

            features = self._fetch_layer(layer_name, url)

            geojson = {
                "type": "FeatureCollection",
                "features": features,
            }
            out_path.write_text(json.dumps(geojson, ensure_ascii=False), encoding="utf-8")
            log.info(
                "corantioquia.saved",
                layer=layer_name,
                path=str(out_path),
                features=len(features),
            )
            paths.append(out_path)

        return paths

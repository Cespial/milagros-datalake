"""Corine Land Cover Colombia ingestor via UPRA ArcGIS REST.

Source: geoservicios.upra.gov.co (Unidad de Planificación Rural Agropecuaria)
Layers (Nivel 3 = most detailed, all Colombia, 1:100,000):
  - clc_2000_2002_nivel_3 / layer 0
  - clc_2005_2009_nivel_3 / layer 0
  - clc_2010_2012_nivel_3 / layer 0

Previous endpoint https://gis.siac.gov.co/arcgis/rest/services/IDEAM/Corine_Land_Cover/FeatureServer
returned DNS failures. New UPRA endpoint confirmed working 2026-04-04.

Outputs one GeoJSON per epoch, clipped to AOI_BBOX.
"""

import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

# UPRA ArcGIS REST endpoint for Corine Land Cover Colombia
UPRA_BASE = "https://geoservicios.upra.gov.co/arcgis/rest/services/uso_suelo_rural"

# Service name → epoch label (UPRA only has 3 published epochs at full-country scale)
EPOCHS = {
    "2000_2002": f"{UPRA_BASE}/clc_2000_2002_nivel_3/MapServer/0",
    "2005_2009": f"{UPRA_BASE}/clc_2005_2009_nivel_3/MapServer/0",
    "2010_2012": f"{UPRA_BASE}/clc_2010_2012_nivel_3/MapServer/0",
}

PAGE_SIZE = 2000


class CorineLcIngestor(BaseIngestor):
    name = "corine_lc"
    source_type = "arcgis_rest"
    data_type = "vector"
    category = "biodiversidad"
    schedule = "once"
    license = "Datos Abiertos Colombia (UPRA / IDEAM)"

    def _fetch_epoch(self, label: str, layer_url: str) -> list[dict]:
        """Paginate through all features for a given CLC epoch layer."""
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

        url = f"{layer_url}/query"
        all_features: list[dict] = []
        offset = 0

        while True:
            params["resultOffset"] = offset
            log.info("corine_lc.fetching", epoch=label, offset=offset)

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
            epoch=label,
            total_features=len(all_features),
        )
        return all_features

    def fetch(self, **kwargs) -> list[Path]:
        paths = []

        for label, layer_url in EPOCHS.items():
            out_path = self.bronze_dir / f"corine_lc_{label}.geojson"
            if out_path.exists():
                log.info("corine_lc.skip_existing", epoch=label)
                paths.append(out_path)
                continue

            features = self._fetch_epoch(label, layer_url)

            geojson = {
                "type": "FeatureCollection",
                "features": features,
            }
            out_path.write_text(
                json.dumps(geojson, ensure_ascii=False), encoding="utf-8"
            )
            log.info(
                "corine_lc.saved",
                epoch=label,
                path=str(out_path),
                features=len(features),
            )
            paths.append(out_path)

        return paths

"""IGAC cartography ingestor via ArcGIS REST FeatureServer / MapServer.

Sources:
  - Municipios: mapas2.igac.gov.co — limites/limites/FeatureServer/1
  - Drenaje Sencillo: mapas2.igac.gov.co — carto/carto100000colombia2019/MapServer/25
  - Curva Nivel: services2.arcgis.com (ArcGIS Online) — carto100000curvasdenivel/FeatureServer/0
  - Via: mapas2.igac.gov.co — carto/carto100000colombia2019/MapServer/15
  - Cuerpo Agua (Laguna+Embalse+Cienaga): mapas2.igac.gov.co — carto/carto100000colombia2019/MapServer

Previous WFS endpoint https://geoportal.igac.gov.co/geoserver/igac/wfs returned 404.
New endpoints confirmed working 2026-04-04.

Outputs one GeoJSON per layer, clipped to AOI_BBOX.
"""

import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

# New IGAC ArcGIS REST base URLs (confirmed public, no auth required)
IGAC_LIMITES_BASE = "https://mapas2.igac.gov.co/server/rest/services/limites/limites/FeatureServer"
IGAC_CARTO_BASE = "https://mapas2.igac.gov.co/server/rest/services/carto/carto100000colombia2019/MapServer"
IGAC_CURVAS_BASE = "https://services2.arcgis.com/RVvWzU3lgJISqdke/arcgis/rest/services/carto100000curvasdenivel/FeatureServer"

# Layer definitions: (service_base, layer_id, output_filename)
LAYERS = [
    (IGAC_LIMITES_BASE, 1, "mpios100k.geojson"),
    (IGAC_CARTO_BASE, 25, "drenaje_sencillo100k.geojson"),
    (IGAC_CURVAS_BASE, 0, "curva_nivel100k.geojson"),
    (IGAC_CARTO_BASE, 15, "via100k.geojson"),
    # Water bodies: merge Laguna(39), Embalse(42), Cienaga(44), Humedal(41)
    (IGAC_CARTO_BASE, 39, "cuerpo_agua100k_laguna.geojson"),
    (IGAC_CARTO_BASE, 42, "cuerpo_agua100k_embalse.geojson"),
    (IGAC_CARTO_BASE, 44, "cuerpo_agua100k_cienaga.geojson"),
]

PAGE_SIZE = 2000


class IgacCartografiaIngestor(BaseIngestor):
    name = "igac_cartografia"
    source_type = "arcgis_rest"
    data_type = "vector"
    category = "geoespacial"
    schedule = "once"
    license = "IGAC Datos Abiertos"

    def _fetch_layer(
        self, service_base: str, layer_id: int, filename: str
    ) -> list[dict]:
        """Paginate through features for a given ArcGIS REST layer inside AOI_BBOX."""
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

        url = f"{service_base}/{layer_id}/query"
        all_features: list[dict] = []
        offset = 0

        while True:
            params["resultOffset"] = offset
            log.info(
                "igac_cartografia.fetching",
                filename=filename,
                layer_id=layer_id,
                offset=offset,
            )

            response = httpx.get(url, params=params, timeout=300)
            response.raise_for_status()
            data = response.json()

            features = data.get("features", [])
            all_features.extend(features)

            # Stop when fewer results than page size (no more pages)
            # Also respect exceededTransferLimit flag for FeatureServer
            exceeded = data.get("properties", {}) or {}
            if isinstance(exceeded, dict):
                exceeded = exceeded.get("exceededTransferLimit", False)
            else:
                exceeded = False

            if len(features) < PAGE_SIZE and not exceeded:
                break

            if len(features) == 0:
                break

            offset += PAGE_SIZE

        log.info(
            "igac_cartografia.layer_complete",
            filename=filename,
            total_features=len(all_features),
        )
        return all_features

    def fetch(self, **kwargs) -> list[Path]:
        paths = []

        # Fetch individual layers
        for service_base, layer_id, filename in LAYERS:
            out_path = self.bronze_dir / filename
            if out_path.exists():
                log.info("igac_cartografia.skip_existing", filename=filename)
                paths.append(out_path)
                continue

            features = self._fetch_layer(service_base, layer_id, filename)

            geojson = {
                "type": "FeatureCollection",
                "features": features,
            }
            out_path.write_text(
                json.dumps(geojson, ensure_ascii=False), encoding="utf-8"
            )
            log.info(
                "igac_cartografia.saved",
                filename=filename,
                path=str(out_path),
                features=len(features),
            )
            paths.append(out_path)

        # Merge water body sub-layers into a single cuerpo_agua100k.geojson
        merged_path = self.bronze_dir / "cuerpo_agua100k.geojson"
        if not merged_path.exists():
            merged_features: list[dict] = []
            for _, _, filename in LAYERS:
                if "cuerpo_agua" in filename:
                    sub_path = self.bronze_dir / filename
                    if sub_path.exists():
                        sub_data = json.loads(sub_path.read_text(encoding="utf-8"))
                        merged_features.extend(sub_data.get("features", []))
            merged_geojson = {
                "type": "FeatureCollection",
                "features": merged_features,
            }
            merged_path.write_text(
                json.dumps(merged_geojson, ensure_ascii=False), encoding="utf-8"
            )
            log.info(
                "igac_cartografia.merged_water_bodies",
                path=str(merged_path),
                features=len(merged_features),
            )
        paths.append(merged_path)

        return paths

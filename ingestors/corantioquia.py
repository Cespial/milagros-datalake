"""CORANTIOQUIA boundaries ingestor via geografico.corantioquia.gov.co ArcGIS REST.

Source: geografico.corantioquia.gov.co (confirmed working 2026-04-04)
Previous endpoint https://sig.corantioquia.gov.co/arcgis/rest/services returned DNS failure.

Layers:
  - jurisdiccion — CORANTIOQUIA municipal jurisdiction (all 8 territorial sections merged)
    Service: Corantioquia_Base_Pub/MapServer, layers 1-8 (Municipios_* per section)
  - POMCA — Plan de Ordenación y Manejo Cuencas Hidrográficas for Rio Grande + Rio Aburra
    Services: Zonificacion_Pomcas_RioGrande, Zonificacion_Pomcas_CC_Rio_Aburra

Notes:
  - POMCA layers do not support bbox-based queries on the server (500/502 errors),
    so they are fetched via OBJECTID-range pagination then filtered to AOI_BBOX client-side.
  - SSL cert is self-signed on this server; verify=False is required.

Outputs one GeoJSON per layer.
"""

import json
import math
from pathlib import Path

import httpx
import structlog
from shapely.geometry import box, mapping

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

CORANTIOQUIA_BASE = "https://geografico.corantioquia.gov.co/arcgis/rest/services"

# Jurisdiccion: 8 territorial sections of CORANTIOQUIA, all are municipal polygons
# Layers 1-8 under Corantioquia_Base_Pub/MapServer
JURISDICCION_LAYERS = [
    (1, "Municipios_Zenufana"),
    (2, "Municipios_Panzenu"),
    (3, "Municipios_Tahamies"),
    (4, "Municipios_Hevexicos"),
    (5, "Municipios_Citara"),
    (6, "Municipios_Cartama"),
    (7, "Municipios_Aburra_Sur"),
    (8, "Municipios_Aburra_Norte"),
]

# POMCA services covering the AOI (northern Antioquia / Altiplano Norte)
POMCA_SERVICES = [
    ("Zonificacion_Pomcas_RioGrande", 0, "POMCA_RioGrande"),
    ("Zonificacion_Pomcas_CC_Rio_Aburra", 0, "POMCA_RioAburra"),
]

VERIFY_SSL = False  # geografico.corantioquia.gov.co uses a self-signed cert
POMCA_BATCH = 100   # features per OBJECTID-range request (server limit for complex polygons)


class CorantioquiaIngestor(BaseIngestor):
    name = "corantioquia"
    source_type = "arcgis_rest"
    data_type = "vector"
    category = "regulatorio"
    schedule = "once"
    license = "CORANTIOQUIA Datos Abiertos"

    def _fetch_layer_bbox(
        self,
        service_path: str,
        layer_id: int,
        layer_name: str,
    ) -> list[dict]:
        """Fetch features via bbox spatial filter (works for municipal layers)."""
        bbox = (
            f"{AOI_BBOX['west']},{AOI_BBOX['south']}"
            f",{AOI_BBOX['east']},{AOI_BBOX['north']}"
        )
        url = f"{CORANTIOQUIA_BASE}/{service_path}/MapServer/{layer_id}/query"
        params: dict = {
            "where": "1=1",
            "geometry": bbox,
            "geometryType": "esriGeometryEnvelope",
            "spatialRel": "esriSpatialRelIntersects",
            "inSR": "4326",
            "outFields": "*",
            "returnGeometry": "true",
            "outSR": "4326",
            "f": "geojson",
        }
        log.info("corantioquia.fetching_bbox", layer=layer_name, url=url)
        response = httpx.get(url, params=params, timeout=120, verify=VERIFY_SSL)
        response.raise_for_status()
        data = response.json()
        features = data.get("features", [])
        log.info("corantioquia.layer_complete", layer=layer_name, total_features=len(features))
        return features

    def _fetch_layer_objectids(
        self,
        service_path: str,
        layer_id: int,
        layer_name: str,
    ) -> list[dict]:
        """Fetch ALL features via OBJECTID-range pagination, then clip to AOI client-side.

        Used for POMCA layers which error out on bbox-based server queries.
        """
        url = f"{CORANTIOQUIA_BASE}/{service_path}/MapServer/{layer_id}/query"
        aoi = box(
            AOI_BBOX["west"], AOI_BBOX["south"],
            AOI_BBOX["east"], AOI_BBOX["north"],
        )

        # Step 1 — get the full list of OBJECTIDs
        ids_resp = httpx.get(
            url,
            params={"where": "1=1", "returnIdsOnly": "true", "f": "json"},
            timeout=60,
            verify=VERIFY_SSL,
        )
        ids_resp.raise_for_status()
        all_ids: list[int] = ids_resp.json().get("objectIds") or []
        log.info(
            "corantioquia.pomca_ids",
            layer=layer_name,
            total_ids=len(all_ids),
        )

        # Step 2 — paginate in POMCA_BATCH-sized chunks by OBJECTID
        all_features: list[dict] = []
        n_batches = math.ceil(len(all_ids) / POMCA_BATCH)

        for i in range(n_batches):
            batch_ids = all_ids[i * POMCA_BATCH: (i + 1) * POMCA_BATCH]
            id_list = ",".join(str(x) for x in batch_ids)
            params = {
                "where": f"OBJECTID IN ({id_list})",
                "outFields": "*",
                "returnGeometry": "true",
                "outSR": "4326",
                "f": "json",
            }
            resp = httpx.get(url, params=params, timeout=60, verify=VERIFY_SSL)
            resp.raise_for_status()
            data = resp.json()
            features = data.get("features", [])

            # Convert Esri JSON features → GeoJSON features and clip to AOI
            for feat in features:
                geom_raw = feat.get("geometry")
                if not geom_raw:
                    continue
                try:
                    # Esri JSON rings → shapely, then convert to GeoJSON geometry
                    geom = self._esri_to_shapely(geom_raw)
                    if geom is None or not geom.intersects(aoi):
                        continue
                    geojson_feat = {
                        "type": "Feature",
                        "geometry": mapping(geom),
                        "properties": feat.get("attributes", {}),
                    }
                    all_features.append(geojson_feat)
                except Exception:
                    pass

            if (i + 1) % 10 == 0:
                log.info(
                    "corantioquia.pomca_progress",
                    layer=layer_name,
                    batch=i + 1,
                    total_batches=n_batches,
                    features_so_far=len(all_features),
                )

        log.info(
            "corantioquia.pomca_complete",
            layer=layer_name,
            total_features=len(all_features),
        )
        return all_features

    @staticmethod
    def _esri_to_shapely(geom_raw: dict):
        """Convert Esri JSON geometry to Shapely geometry."""
        from shapely.geometry import Polygon
        try:
            rings = geom_raw.get("rings")
            if rings:
                # Esri Polygon — first ring is exterior, rest are holes
                exterior = rings[0]
                holes = rings[1:] if len(rings) > 1 else []
                return Polygon(exterior, holes)
        except Exception:
            pass
        return None

    def fetch(self, **kwargs) -> list[Path]:
        paths = []

        # ── Jurisdiccion: merge all 8 municipal section layers ──────────────
        jurisdiccion_path = self.bronze_dir / "jurisdiccion.geojson"
        if jurisdiccion_path.exists():
            log.info("corantioquia.skip_existing", layer="jurisdiccion")
            paths.append(jurisdiccion_path)
        else:
            all_features: list[dict] = []
            for layer_id, section_name in JURISDICCION_LAYERS:
                try:
                    features = self._fetch_layer_bbox(
                        "Corantioquia_Base_Pub", layer_id, section_name
                    )
                    all_features.extend(features)
                except Exception as exc:
                    log.warning(
                        "corantioquia.section_failed",
                        section=section_name,
                        error=str(exc),
                    )

            geojson = {"type": "FeatureCollection", "features": all_features}
            jurisdiccion_path.write_text(
                json.dumps(geojson, ensure_ascii=False), encoding="utf-8"
            )
            log.info(
                "corantioquia.saved",
                layer="jurisdiccion",
                path=str(jurisdiccion_path),
                features=len(all_features),
            )
            paths.append(jurisdiccion_path)

        # ── POMCA: OBJECTID-paginated fetch + client-side AOI clip ──────────
        pomca_path = self.bronze_dir / "pomca.geojson"
        if pomca_path.exists():
            log.info("corantioquia.skip_existing", layer="POMCA")
            paths.append(pomca_path)
        else:
            pomca_features: list[dict] = []
            for service_name, layer_id, pomca_label in POMCA_SERVICES:
                try:
                    features = self._fetch_layer_objectids(
                        service_name, layer_id, pomca_label
                    )
                    pomca_features.extend(features)
                except Exception as exc:
                    log.warning(
                        "corantioquia.pomca_failed",
                        service=service_name,
                        error=str(exc),
                    )

            geojson = {"type": "FeatureCollection", "features": pomca_features}
            pomca_path.write_text(
                json.dumps(geojson, ensure_ascii=False), encoding="utf-8"
            )
            log.info(
                "corantioquia.saved",
                layer="POMCA",
                path=str(pomca_path),
                features=len(pomca_features),
            )
            paths.append(pomca_path)

        return paths

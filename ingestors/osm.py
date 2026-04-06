"""OpenStreetMap infrastructure ingestor via Overpass API.

Downloads roads, power lines, and waterways within the AOI bounding box.
Output: one GeoJSON FeatureCollection per feature type.

Overpass API: https://overpass-api.de/api/interpreter
"""

import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# Overpass QL query templates — bbox order: south,west,north,east
_BBOX = "{south},{west},{north},{east}"

QUERIES = {
    "roads": (
        "[out:json][timeout:180];"
        "(way['highway'~'primary|secondary|tertiary|trunk|motorway|residential']"
        "({south},{west},{north},{east});"
        ");out geom qt;"
    ),
    "power": (
        "[out:json][timeout:120];"
        "(way['power']({south},{west},{north},{east});"
        "node['power']({south},{west},{north},{east});"
        ");out geom qt;"
    ),
    "waterways": (
        "[out:json][timeout:120];"
        "(way['waterway']({south},{west},{north},{east});"
        ");out geom qt;"
    ),
    "buildings": (
        "[out:json][timeout:120];"
        "(way['building']({south},{west},{north},{east});"
        ");out geom qt;"
    ),
}


def _overpass_to_geojson(elements: list[dict]) -> dict:
    """Convert Overpass JSON elements to a minimal GeoJSON FeatureCollection.

    Supports both 'out geom' format (geometry embedded directly in way as
    'geometry' array of {lat, lon} dicts) and classic 'out body;>;out skel'
    format (separate node elements with lat/lon resolved via nd refs).
    """
    # Build node id → [lon, lat] lookup (for classic format)
    nodes: dict[int, list[float]] = {}
    for el in elements:
        if el["type"] == "node" and "lat" in el and "lon" in el:
            nodes[el["id"]] = [el["lon"], el["lat"]]

    features = []
    for el in elements:
        tags = el.get("tags", {})
        if el["type"] == "node" and tags:
            # Tagged node (e.g. power substation, junction)
            if el["id"] in nodes:
                features.append({
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": nodes[el["id"]]},
                    "properties": {"osm_id": el["id"], "osm_type": "node", **tags},
                })
        elif el["type"] == "way":
            # 'out geom' format: geometry is embedded as list of {lat, lon}
            geom_list = el.get("geometry")
            if geom_list:
                coords = [[pt["lon"], pt["lat"]] for pt in geom_list if "lat" in pt and "lon" in pt]
            else:
                # Classic format: resolve nd refs from nodes dict
                coords = [nodes[nid] for nid in el.get("nd", []) if nid in nodes]

            if len(coords) >= 2:
                features.append({
                    "type": "Feature",
                    "geometry": {"type": "LineString", "coordinates": coords},
                    "properties": {"osm_id": el["id"], "osm_type": "way", **tags},
                })

    return {"type": "FeatureCollection", "features": features}


class OsmIngestor(BaseIngestor):
    name = "osm"
    source_type = "api"
    data_type = "vector"
    category = "infraestructura"
    schedule = "monthly"
    license = "ODbL (OpenStreetMap contributors)"

    def _run_query(self, feature_type: str, query_template: str) -> dict:
        """Execute an Overpass query and return Overpass JSON dict."""
        query = query_template.format(
            south=AOI_BBOX["south"],
            west=AOI_BBOX["west"],
            north=AOI_BBOX["north"],
            east=AOI_BBOX["east"],
        )
        log.info("osm.querying", feature=feature_type, query_len=len(query))
        resp = httpx.post(
            OVERPASS_URL,
            data={"data": query},
            timeout=180,
            follow_redirects=True,
        )
        resp.raise_for_status()
        return resp.json()

    def fetch(self, **kwargs) -> list[Path]:
        paths = []

        for feature_type, query_template in QUERIES.items():
            out_path = self.bronze_dir / f"osm_{feature_type}.geojson"

            if out_path.exists():
                log.info("osm.skip_existing", feature=feature_type)
                paths.append(out_path)
                continue

            try:
                raw = self._run_query(feature_type, query_template)
                elements = raw.get("elements", [])
                log.info("osm.elements_received", feature=feature_type, count=len(elements))

                geojson = _overpass_to_geojson(elements)
                out_path.write_text(
                    json.dumps(geojson, ensure_ascii=False, separators=(",", ":")),
                    encoding="utf-8",
                )
                log.info(
                    "osm.saved",
                    feature=feature_type,
                    path=str(out_path),
                    features=len(geojson["features"]),
                )
                paths.append(out_path)

            except httpx.HTTPStatusError as exc:
                log.warning("osm.http_error", feature=feature_type, status=exc.response.status_code, error=str(exc))
            except Exception as exc:
                log.warning("osm.feature_failed", feature=feature_type, error=str(exc))

        return paths

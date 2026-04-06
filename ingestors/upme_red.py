"""UPME electrical grid — STN transmission lines and substations.

Sources (tried in order):
  1. datos.gov.co dataset `r3em-7gvg` — Líneas de transmisión STN (UPME)
  2. datos.gov.co dataset `6vmg-k3pv` — Subestaciones STN (UPME)
  3. Fallback: known UPME ArcGIS REST service for the energy atlas

Downloads transmission lines and substations, saves as GeoJSON.
Files: stn_lineas.geojson, stn_subestaciones.geojson
"""

import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

BASE_URL = "https://www.datos.gov.co/resource"

# datos.gov.co dataset IDs — both are UPME infrastructure layers
DATASETS = {
    "stn_lineas": "r3em-7gvg",
    "stn_subestaciones": "6vmg-k3pv",
}

# Alternative known IDs (UPME PARATEC / SIG-AME)
ALT_DATASETS = {
    "stn_lineas": ["r3em-7gvg", "7k9m-7bpv", "n3p2-7cyt"],
    "stn_subestaciones": ["6vmg-k3pv", "h9qu-k4xc", "9mey-c8s8"],
}

# UPME ArcGIS REST fallback
UPME_ARCGIS = (
    "https://srvags.sgc.gov.co/arcgis/rest/services"
    "/SIAE_BienesEnergeticos/FeatureServer/{layer}/query"
)
UPME_LAYERS = {"stn_lineas": 0, "stn_subestaciones": 1}

PAGE_SIZE = 50_000


class UpmeRedIngestor(BaseIngestor):
    name = "upme_red"
    source_type = "api"
    data_type = "vector"
    category = "infraestructura"
    schedule = "annual"
    license = "Datos Abiertos Colombia (UPME)"

    def fetch(self, **kwargs) -> list[Path]:
        paths: list[Path] = []

        for layer_name, primary_id in DATASETS.items():
            out_path = self.bronze_dir / f"{layer_name}.geojson"
            if out_path.exists():
                log.info("upme_red.skip_existing", layer=layer_name)
                paths.append(out_path)
                continue

            features = self._fetch_layer(layer_name, primary_id)
            geojson = {"type": "FeatureCollection", "features": features}
            out_path.write_text(json.dumps(geojson, ensure_ascii=False), encoding="utf-8")
            log.info("upme_red.saved", layer=layer_name, features=len(features), path=str(out_path))
            paths.append(out_path)

        return paths

    def _fetch_layer(self, layer_name: str, primary_id: str) -> list[dict]:
        # Try all known datos.gov.co IDs
        for dataset_id in ALT_DATASETS.get(layer_name, [primary_id]):
            records = self._try_datosgov(dataset_id)
            if records:
                return records
            log.debug("upme_red.datosgov_empty", layer=layer_name, id=dataset_id)

        # Fallback: UPME/SGC ArcGIS REST
        layer_idx = UPME_LAYERS.get(layer_name, 0)
        features = self._try_arcgis(layer_idx)
        if features:
            return features

        log.warning("upme_red.all_sources_empty", layer=layer_name)
        return []

    def _try_datosgov(self, dataset_id: str) -> list[dict]:
        url = f"{BASE_URL}/{dataset_id}.json"
        all_records: list[dict] = []
        offset = 0

        while True:
            try:
                resp = httpx.get(
                    url,
                    params={"$limit": PAGE_SIZE, "$offset": offset},
                    timeout=120,
                )
                resp.raise_for_status()
                page = resp.json()
            except Exception as exc:
                log.warning("upme_red.datosgov_failed", id=dataset_id, error=str(exc))
                return all_records

            if not page:
                break

            all_records.extend(page)
            if len(page) < PAGE_SIZE:
                break
            offset += PAGE_SIZE

        # Wrap as GeoJSON features
        features = [
            {"type": "Feature", "geometry": None, "properties": r}
            for r in all_records
        ]
        return features

    def _try_arcgis(self, layer_idx: int) -> list[dict]:
        url = UPME_ARCGIS.format(layer=layer_idx)
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
            "resultRecordCount": 5000,
        }

        all_features = []
        offset = 0

        while True:
            params["resultOffset"] = offset
            try:
                resp = httpx.get(url, params=params, timeout=120)
                resp.raise_for_status()
                data = resp.json()
            except Exception as exc:
                log.warning("upme_red.arcgis_failed", layer=layer_idx, error=str(exc))
                break

            features = data.get("features", [])
            all_features.extend(features)
            if len(features) < 5000:
                break
            offset += 5000

        return all_features

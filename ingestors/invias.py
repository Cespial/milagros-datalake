"""INVIAS road network ingestor.

Source: INVIAS ArcGIS Hub open data portal
  Primary:  https://inviasopendata-invias.opendata.arcgis.com/api/v3/datasets
  Fallback: datos.gov.co datasets for vias / red vial

Downloads road network GeoJSON for features that intersect the AOI_BBOX.
Saves: red_vial.geojson  (primary or fallback result)
"""

import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

# ArcGIS Hub API — search for the road network layer
HUB_SEARCH_URL = "https://inviasopendata-invias.opendata.arcgis.com/api/v3/datasets"

# Known INVIAS ArcGIS Feature Service endpoint (red vial nacional)
ARCGIS_SERVICE_URL = (
    "https://hermes2.invias.gov.co/arcgis/rest/services"
    "/CartografiaBasica/mapa_referencia/FeatureServer/0/query"
)

# datos.gov.co fallback — INVIAS red vial nacional (ID confirmed 2026-04)
DATOSGOV_URL = "https://www.datos.gov.co/resource/ie7y-asdn.json"

PAGE_SIZE = 5000


class InviasIngestor(BaseIngestor):
    name = "invias"
    source_type = "arcgis_rest"
    data_type = "vector"
    category = "infraestructura"
    schedule = "annual"
    license = "Datos Abiertos Colombia (INVIAS)"

    def fetch(self, **kwargs) -> list[Path]:
        out_path = self.bronze_dir / "red_vial.geojson"
        if out_path.exists():
            log.info("invias.skip_existing")
            return [out_path]

        # Try 1: ArcGIS Hub search to discover the right layer ID
        features = self._try_hub_search()

        # Try 2: direct ArcGIS REST service
        if not features:
            features = self._try_arcgis_rest()

        # Try 3: datos.gov.co fallback
        if not features:
            features = self._try_datosgov()

        geojson = {
            "type": "FeatureCollection",
            "features": features,
        }
        out_path.write_text(json.dumps(geojson, ensure_ascii=False), encoding="utf-8")
        log.info("invias.saved", path=str(out_path), features=len(features))
        return [out_path]

    def _try_hub_search(self) -> list[dict]:
        """Search ArcGIS Hub for INVIAS road network datasets."""
        log.info("invias.hub_search_attempt")
        try:
            resp = httpx.get(
                HUB_SEARCH_URL,
                params={"q": "red vial", "limit": 5},
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            datasets = data.get("data", [])
            if not datasets:
                log.warning("invias.hub_no_datasets")
                return []

            # Use the first dataset's GeoJSON download link
            first = datasets[0]
            ds_id = first.get("id", "")
            log.info("invias.hub_found_dataset", id=ds_id, title=first.get("attributes", {}).get("name"))

            # Try to get the FeatureServer URL
            links = first.get("links", [])
            feature_server = None
            for link in links:
                if "FeatureServer" in link.get("href", ""):
                    feature_server = link["href"] + "/0/query"
                    break

            if feature_server:
                return self._query_feature_server(feature_server)

        except Exception as exc:
            log.warning("invias.hub_failed", error=str(exc))
        return []

    def _try_arcgis_rest(self) -> list[dict]:
        """Query the INVIAS Hermes2 ArcGIS REST FeatureServer directly."""
        log.info("invias.arcgis_rest_attempt")
        return self._query_feature_server(ARCGIS_SERVICE_URL)

    def _query_feature_server(self, url: str) -> list[dict]:
        """Page through an ArcGIS FeatureServer with AOI spatial filter."""
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
            log.info("invias.arcgis_page", offset=offset, url=url)
            try:
                resp = httpx.get(url, params=params, timeout=120)
                resp.raise_for_status()
                data = resp.json()
            except Exception as exc:
                log.warning("invias.arcgis_page_failed", offset=offset, error=str(exc))
                break

            features = data.get("features", [])
            all_features.extend(features)
            log.info("invias.arcgis_page_done", offset=offset, count=len(features), total=len(all_features))

            if len(features) < PAGE_SIZE:
                break
            offset += PAGE_SIZE

        return all_features

    def _try_datosgov(self) -> list[dict]:
        """Fallback: fetch road records from datos.gov.co (INVIAS red vial, dataset ie7y-asdn).

        This dataset has a 'territorial' column where territorial=1 means Antioquia.
        Filter by territorial=1 for AOI relevance.
        """
        log.info("invias.datosgov_attempt")
        records = []
        offset = 0
        limit = 50_000

        while True:
            try:
                resp = httpx.get(
                    DATOSGOV_URL,
                    params={
                        "$limit": limit,
                        "$offset": offset,
                        "$where": "territorial = '1'",  # 1 = Antioquia
                    },
                    timeout=120,
                )
                resp.raise_for_status()
                page = resp.json()
            except Exception as exc:
                log.warning("invias.datosgov_failed", offset=offset, error=str(exc))
                break

            if not page:
                break

            records.extend(page)
            log.info("invias.datosgov_page", offset=offset, count=len(page), total=len(records))

            if len(page) < limit:
                break
            offset += limit

        # Convert to GeoJSON features — geometry comes from 'multiline' field if present
        features = []
        for r in records:
            geom = None
            if "multiline" in r and r["multiline"]:
                try:
                    geom = r.pop("multiline")
                except Exception:
                    pass
            features.append({"type": "Feature", "geometry": geom, "properties": r})
        return features

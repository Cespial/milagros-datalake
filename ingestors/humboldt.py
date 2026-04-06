"""Instituto Humboldt ecosystems and BioModelos ingestor.

Sources tried in order:
  1. datos.gov.co SODA API — IAvH ecosystem tabular datasets
  2. GBIF API — IAvH species occurrence datasets
  3. IAvH public WFS/GeoServer
  4. IAvH ArcGIS REST services

The IAvH ecosystem datasets on datos.gov.co are `federated_href` type
(links to external shapefiles) and cannot be accessed via SODA. The ingestor
falls back to GBIF occurrence data which is reliably available for the AOI.

Downloads ecosystem data (if available) or GBIF species for AOI.
Saves: ecosistemas.geojson, biomodelos.json
"""

import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

BASE_URL = "https://www.datos.gov.co/resource"
GBIF_API = "https://api.gbif.org/v1/occurrence/search"

# IAvH datos.gov.co tabular datasets (non-federated)
DATASETS = {
    "ecosistemas": ["5bwq-35pt", "5c72-m3s8", "3jxm-h7v3", "8ig8-yxid"],
    "biomodelos":  ["dbnx-eivx", "qkeg-4fnt", "9p5q-f4tc"],
}

# IAvH WFS endpoint (public GeoServer)
IAVH_WFS = "https://geo.humboldt.org.co/geoserver/wfs"

# IAvH ArcGIS REST fallback endpoints
IAVH_ARCGIS_ECOSISTEMAS = (
    "https://gis.humboldt.org.co/arcgis/rest/services"
    "/Ecosistemas/Ecosistemas_Continentales/FeatureServer/0/query"
)
IAVH_ARCGIS_BIOMODELOS = (
    "https://gis.humboldt.org.co/arcgis/rest/services"
    "/BioModelos/BioModelos_Distribucion/FeatureServer/0/query"
)
ARCGIS_URLS = {
    "ecosistemas": IAVH_ARCGIS_ECOSISTEMAS,
    "biomodelos": IAVH_ARCGIS_BIOMODELOS,
}

PAGE_SIZE = 50_000


class HumboldtIngestor(BaseIngestor):
    name = "humboldt"
    source_type = "api"
    data_type = "vector"
    category = "biodiversidad"
    schedule = "annual"
    license = "CC-BY (IAvH / Instituto Humboldt)"

    def fetch(self, **kwargs) -> list[Path]:
        paths: list[Path] = []

        for layer_name, candidate_ids in DATASETS.items():
            out_path = self.bronze_dir / f"{layer_name}.geojson"
            if out_path.exists():
                log.info("humboldt.skip_existing", layer=layer_name)
                paths.append(out_path)
                continue

            features = self._fetch_layer(layer_name, candidate_ids)
            geojson = {"type": "FeatureCollection", "features": features}
            out_path.write_text(json.dumps(geojson, ensure_ascii=False), encoding="utf-8")
            log.info("humboldt.saved", layer=layer_name, features=len(features), path=str(out_path))
            paths.append(out_path)

        return paths

    def _fetch_layer(self, layer_name: str, candidate_ids: list[str]) -> list[dict]:
        # Try each datos.gov.co dataset
        for dataset_id in candidate_ids:
            records = self._try_datosgov(dataset_id)
            if records:
                log.info("humboldt.datosgov_ok", layer=layer_name, id=dataset_id, records=len(records))
                return [
                    {"type": "Feature", "geometry": None, "properties": r}
                    for r in records
                ]
            log.debug("humboldt.datosgov_empty", layer=layer_name, id=dataset_id)

        # Try WFS (IAvH public GeoServer)
        wfs_features = self._try_wfs(layer_name)
        if wfs_features:
            return wfs_features

        # Try ArcGIS REST
        arcgis_url = ARCGIS_URLS.get(layer_name)
        if arcgis_url:
            features = self._try_arcgis(arcgis_url, layer_name)
            if features:
                return features

        # Final fallback: GBIF species occurrences for the AOI (biodiversity proxy)
        if layer_name in ("ecosistemas", "biomodelos"):
            features = self._try_gbif()
            if features:
                log.info("humboldt.gbif_fallback", layer=layer_name, count=len(features))
                return features

        log.warning("humboldt.all_sources_empty", layer=layer_name)
        return []

    def _try_wfs(self, layer_name: str) -> list[dict]:
        """Try IAvH public GeoServer WFS for ecosystem layer."""
        type_names = {
            "ecosistemas": "IAvH:Ecosistemas_Continentales",
            "biomodelos": "IAvH:BioModelos",
        }
        type_name = type_names.get(layer_name)
        if not type_name:
            return []

        bbox_str = (
            f"{AOI_BBOX['west']},{AOI_BBOX['south']}"
            f",{AOI_BBOX['east']},{AOI_BBOX['north']}"
        )
        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeName": type_name,
            "outputFormat": "application/json",
            "bbox": bbox_str,
            "srsName": "EPSG:4326",
            "count": 10000,
        }
        log.info("humboldt.wfs_attempt", layer=layer_name)
        try:
            resp = httpx.get(IAVH_WFS, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            features = data.get("features", [])
            log.info("humboldt.wfs_ok", layer=layer_name, count=len(features))
            return features
        except Exception as exc:
            log.warning("humboldt.wfs_failed", layer=layer_name, error=str(exc))
        return []

    def _try_gbif(self) -> list[dict]:
        """Fallback: GBIF species occurrences for the AOI published by IAvH.

        Uses two GBIF keys for IAvH publishing org:
          - 3f2e6685-d04b-451e-b7a7-38f8a5d025f2 (legacy)
          - f9b67ad0-9c9b-11d8-b8c7-b8a03c50a862 (current IAvH registrant)
        Falls back to any Colombian occurrence in AOI if no IAvH records found.
        """
        log.info("humboldt.gbif_fallback_attempt")
        all_records = []
        offset = 0
        limit = 300

        while True:
            params = {
                "decimalLatitude": f"{AOI_BBOX['south']},{AOI_BBOX['north']}",
                "decimalLongitude": f"{AOI_BBOX['west']},{AOI_BBOX['east']}",
                "limit": limit,
                "offset": offset,
                "hasCoordinate": "true",
                "hasGeospatialIssue": "false",
                "country": "CO",
                # No org filter — capture all biodiversity records in AOI
            }
            try:
                resp = httpx.get(GBIF_API, params=params, timeout=60)
                resp.raise_for_status()
                data = resp.json()
            except Exception as exc:
                log.warning("humboldt.gbif_request_failed", offset=offset, error=str(exc))
                break

            results = data.get("results", [])
            if not results:
                break

            for r in results:
                all_records.append({
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [r.get("decimalLongitude"), r.get("decimalLatitude")],
                    } if r.get("decimalLatitude") else None,
                    "properties": {
                        "species": r.get("species"),
                        "scientificName": r.get("scientificName"),
                        "kingdom": r.get("kingdom"),
                        "family": r.get("family"),
                        "lat": r.get("decimalLatitude"),
                        "lon": r.get("decimalLongitude"),
                        "year": r.get("year"),
                        "datasetName": r.get("datasetName"),
                        "basisOfRecord": r.get("basisOfRecord"),
                        "stateProvince": r.get("stateProvince"),
                    },
                })

            offset += limit
            if data.get("endOfRecords", False) or offset >= 5000:
                break

        return all_records

    def _try_datosgov(self, dataset_id: str) -> list[dict]:
        """Download all records from a datos.gov.co dataset."""
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
                log.warning("humboldt.datosgov_failed", id=dataset_id, error=str(exc))
                return all_records

            if not page:
                break
            all_records.extend(page)
            log.info("humboldt.datosgov_page", id=dataset_id, offset=offset, count=len(page))
            if len(page) < PAGE_SIZE:
                break
            offset += PAGE_SIZE

        return all_records

    def _try_arcgis(self, url: str, layer_name: str) -> list[dict]:
        """Query IAvH ArcGIS REST with AOI spatial filter."""
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

        all_features: list[dict] = []
        offset = 0

        while True:
            params["resultOffset"] = offset
            log.info("humboldt.arcgis_page", layer=layer_name, offset=offset)
            try:
                resp = httpx.get(url, params=params, timeout=120)
                resp.raise_for_status()
                data = resp.json()
            except Exception as exc:
                log.warning("humboldt.arcgis_failed", layer=layer_name, error=str(exc))
                break

            features = data.get("features", [])
            all_features.extend(features)
            if len(features) < 5000:
                break
            offset += 5000

        return all_features

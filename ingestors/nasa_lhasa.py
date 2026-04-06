"""NASA LHASA landslide nowcast / susceptibility ingestor.

Primary source  : GEE — NASA Global Landslide Susceptibility
  Asset: projects/sat-io/open-datasets/GLOBAL_LANDSLIDE_HAZARD/GLOBAL_LANDSLIDE_SUSCEPTIBILITY
  (Community contributed via sat-io, derived from NASA/USGS LHASA v2)

Secondary source: NASA NCCS ArcGIS REST Service
  https://maps.nccs.nasa.gov/arcgis/rest/services/lhasa/lhasa/MapServer/0/query

Fallback        : If neither source is accessible, writes a metadata stub JSON
  with reference URLs for manual download of the LHASA v2 global TIF.

References:
  Kirschbaum, D. et al. (2015) — LHASA model
  https://gpm.nasa.gov/landslides/
  https://maps.nccs.nasa.gov/arcgis/rest/services/lhasa/
"""

import json
import os
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

# GEE asset (community dataset via sat-io)
GEE_SUSCEPTIBILITY_ASSET = (
    "projects/sat-io/open-datasets/GLOBAL_LANDSLIDE_HAZARD"
    "/GLOBAL_LANDSLIDE_SUSCEPTIBILITY"
)

# Alternative GEE asset path tried if primary fails
GEE_ALT_ASSETS = [
    "NASA/GLOBAL_LANDSLIDE/SUSCEPTIBILITY",
    "users/sat-io/GLOBAL_LANDSLIDE_SUSCEPTIBILITY",
]

DOWNLOAD_SCALE = 1000  # ~1 km — susceptibility is coarse global product

# NASA NCCS ArcGIS REST endpoint for LHASA
NCCS_URL = (
    "https://maps.nccs.nasa.gov/arcgis/rest/services"
    "/lhasa/lhasa/MapServer/0/query"
)

FALLBACK_METADATA = {
    "dataset": "NASA LHASA v2 — Landslide Hazard Assessment for Situational Awareness",
    "description": (
        "Global near-real-time landslide susceptibility and nowcasting model. "
        "Combines rainfall triggers (IMERG), slope/geology, road proximity."
    ),
    "access_urls": {
        "viewer": "https://maps.nccs.nasa.gov/arcgis/apps/webappviewer/index.html?id=824ea5864ec8423fb985b33ee6bc05b7",
        "arcgis_rest": "https://maps.nccs.nasa.gov/arcgis/rest/services/lhasa/",
        "global_tif": "https://gpm.nasa.gov/landslides/data.html",
        "gee_catalog": "https://developers.google.com/earth-engine/datasets",
        "sat_io_gee": "https://samapriya.github.io/awesome-gee-community-datasets/projects/lhasa/",
    },
    "variables": [
        "susceptibility_class",   # 1=low, 2=moderate, 3=high, 4=very_high
        "nowcast_hazard",         # real-time hazard combining susceptibility + rain trigger
    ],
    "temporal_coverage": "2007-present (nowcast); static susceptibility map",
    "spatial_resolution": "~1 km (susceptibility), ~10 km (nowcast)",
    "license": "NASA Open Data",
    "aoi_bbox": AOI_BBOX,
    "note": (
        "GEE community asset not accessible or AOI returned no data. "
        "Download the global susceptibility GeoTIFF manually from the links above "
        "and clip to AOI bbox (west=-75.80, south=6.25, east=-75.25, north=6.70)."
    ),
}


class NasaLhasaIngestor(BaseIngestor):
    name = "nasa_lhasa"
    source_type = "gee"
    data_type = "raster"
    category = "geologia"
    schedule = "once"
    license = "NASA Open Data"

    def _try_gee(self, aoi) -> bytes | None:
        """Attempt to download susceptibility raster from GEE. Returns bytes or None."""
        import ee

        asset_attempts = [GEE_SUSCEPTIBILITY_ASSET] + GEE_ALT_ASSETS
        for asset in asset_attempts:
            try:
                log.info("nasa_lhasa.gee_trying", asset=asset)
                img = ee.Image(asset).clip(aoi)

                # Get first band name
                band_names = img.bandNames().getInfo()
                if not band_names:
                    log.warning("nasa_lhasa.gee_no_bands", asset=asset)
                    continue

                band = band_names[0]
                log.info("nasa_lhasa.gee_band", asset=asset, band=band)

                url = img.select(band).getDownloadURL({
                    "scale": DOWNLOAD_SCALE,
                    "region": aoi,
                    "format": "GEO_TIFF",
                    "crs": "EPSG:4326",
                })
                resp = httpx.get(url, timeout=300, follow_redirects=True)
                resp.raise_for_status()
                log.info("nasa_lhasa.gee_success", asset=asset, size_mb=round(len(resp.content) / 1e6, 2))
                return resp.content
            except Exception as exc:
                log.warning("nasa_lhasa.gee_asset_failed", asset=asset, error=str(exc))

        return None

    def _try_nccs_arcgis(self) -> list[dict] | None:
        """Query NASA NCCS ArcGIS for LHASA features in AOI. Returns features list or None."""
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
            "resultRecordCount": 1000,
        }
        log.info("nasa_lhasa.nccs_query")
        try:
            resp = httpx.get(NCCS_URL, params=params, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            features = data.get("features", [])
            log.info("nasa_lhasa.nccs_features", count=len(features))
            return features if features else None
        except Exception as exc:
            log.warning("nasa_lhasa.nccs_failed", error=str(exc))
            return None

    def fetch(self, **kwargs) -> list[Path]:
        paths = []

        tif_path = self.bronze_dir / "lhasa_susceptibility.tif"
        geojson_path = self.bronze_dir / "lhasa_nowcast_aoi.geojson"
        meta_path = self.bronze_dir / "nasa_lhasa_metadata.json"

        # ── Try GEE susceptibility raster ─────────────────────────────────────
        if tif_path.exists():
            log.info("nasa_lhasa.skip_existing_tif")
            paths.append(tif_path)
        else:
            try:
                import ee
                ee.Initialize(project=os.environ.get("GEE_PROJECT"))
                aoi = ee.Geometry.BBox(
                    AOI_BBOX["west"], AOI_BBOX["south"],
                    AOI_BBOX["east"], AOI_BBOX["north"],
                )
                tif_data = self._try_gee(aoi)
                if tif_data:
                    tif_path.write_bytes(tif_data)
                    paths.append(tif_path)
            except Exception as exc:
                log.warning("nasa_lhasa.gee_init_failed", error=str(exc))

        # ── Try NCCS ArcGIS REST ──────────────────────────────────────────────
        if geojson_path.exists():
            log.info("nasa_lhasa.skip_existing_geojson")
            paths.append(geojson_path)
        else:
            features = self._try_nccs_arcgis()
            if features:
                geojson = {"type": "FeatureCollection", "features": features}
                geojson_path.write_text(
                    json.dumps(geojson, ensure_ascii=False), encoding="utf-8"
                )
                log.info("nasa_lhasa.geojson_saved", features=len(features))
                paths.append(geojson_path)

        # ── Fallback: always write metadata ───────────────────────────────────
        if not meta_path.exists():
            meta_path.write_text(json.dumps(FALLBACK_METADATA, indent=2, ensure_ascii=False))
            log.info("nasa_lhasa.metadata_saved")
        paths.append(meta_path)

        if not paths or (len(paths) == 1 and paths[0] == meta_path):
            log.warning(
                "nasa_lhasa.only_metadata",
                msg="Neither GEE nor NCCS returned data. Metadata stub written.",
            )

        return paths

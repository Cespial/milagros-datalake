"""Copernicus water quality indices via GEE (Sentinel-3 OLCI).

Primary source  : GEE COPERNICUS/S3/OLCI — Sentinel-3 OLCI EFR Level-1b
                  Derives chlorophyll proxy (MERIS band ratio Oa08/Oa06) and
                  turbidity proxy (Oa17 reflectance) as annual medians.

Fallback        : If GEE returns no imagery (S3 started ~2017, limited coverage
                  over small AOIs), writes a metadata stub JSON pointing to the
                  Copernicus Marine Environment Monitoring Service (CMEMS) portal
                  for manual download.

Reference:
  https://developers.google.com/earth-engine/datasets/catalog/COPERNICUS_S3_OLCI
"""

import json
import os
from pathlib import Path

import ee
import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

COLLECTION = "COPERNICUS/S3/OLCI"
DOWNLOAD_SCALE = 300  # OLCI ~300 m native
START_YEAR = 2017
END_YEAR = 2025

# Fallback metadata for manual access
FALLBACK_METADATA = {
    "dataset": "Copernicus Global Land Service — Water Quality (CGLS-WQ)",
    "portal": "https://land.copernicus.eu/global/products/wq",
    "cmems": "https://data.marine.copernicus.eu/",
    "gee_catalog": "https://developers.google.com/earth-engine/datasets/catalog/COPERNICUS_S3_OLCI",
    "variables": ["chlorophyll_a", "turbidity", "suspended_matter", "cdom"],
    "spatial_resolution": "300m",
    "temporal_resolution": "daily / monthly composites",
    "note": (
        "Sentinel-3 OLCI coverage over small inland AOIs is sparse. "
        "For reliable time series, access CMEMS or Copernicus Land Service directly."
    ),
}


class CopernicusWqIngestor(BaseIngestor):
    name = "copernicus_wq"
    source_type = "gee"
    data_type = "raster"
    category = "calidad_agua"
    schedule = "annual"
    license = "Copernicus Open Access"

    def _download_tif(self, image: ee.Image, aoi: ee.Geometry, band: str, out_path: Path) -> bool:
        """Download a single-band GeoTIFF. Returns True on success."""
        try:
            url = image.select(band).getDownloadURL({
                "scale": DOWNLOAD_SCALE,
                "region": aoi,
                "format": "GEO_TIFF",
                "crs": "EPSG:4326",
            })
            resp = httpx.get(url, timeout=300, follow_redirects=True)
            resp.raise_for_status()
            out_path.write_bytes(resp.content)
            log.info("copernicus_wq.tif_saved", path=str(out_path), size_mb=round(len(resp.content) / 1e6, 2))
            return True
        except Exception as exc:
            log.warning("copernicus_wq.tif_failed", band=band, error=str(exc))
            return False

    def fetch(self, **kwargs) -> list[Path]:
        ee.Initialize(project=os.environ.get("GEE_PROJECT"))

        aoi = ee.Geometry.BBox(
            AOI_BBOX["west"], AOI_BBOX["south"],
            AOI_BBOX["east"], AOI_BBOX["north"],
        )

        start_year = kwargs.get("start_year", START_YEAR)
        end_year = kwargs.get("end_year", END_YEAR)
        paths = []
        any_gee_success = False

        for year in range(start_year, end_year + 1):
            chl_path = self.bronze_dir / f"chlorophyll_proxy_{year}.tif"
            turb_path = self.bronze_dir / f"turbidity_proxy_{year}.tif"

            if chl_path.exists() and turb_path.exists():
                log.info("copernicus_wq.skip_existing", year=year)
                paths.extend([chl_path, turb_path])
                any_gee_success = True
                continue

            log.info("copernicus_wq.processing", year=year)

            try:
                col = (
                    ee.ImageCollection(COLLECTION)
                    .filterBounds(aoi)
                    .filterDate(f"{year}-01-01", f"{year}-12-31")
                )
                count = col.size().getInfo()
                if count == 0:
                    log.warning("copernicus_wq.no_images", year=year)
                    continue

                log.info("copernicus_wq.image_count", year=year, count=count)
                median = col.median().clip(aoi)

                # Chlorophyll proxy: Oa08 (665 nm) / Oa06 (560 nm) band ratio
                # Both bands contain radiance values in the OLCI GEE collection
                chl_proxy = median.normalizedDifference(["Oa08_radiance", "Oa06_radiance"]).rename("chl_proxy")
                if not chl_path.exists():
                    if self._download_tif(chl_proxy, aoi, "chl_proxy", chl_path):
                        paths.append(chl_path)
                        any_gee_success = True

                # Turbidity proxy: Oa17 (865 nm) reflectance (NIR — sensitive to suspended sediment)
                turb_proxy = median.select("Oa17_radiance").rename("turb_proxy")
                if not turb_path.exists():
                    if self._download_tif(turb_proxy, aoi, "turb_proxy", turb_path):
                        paths.append(turb_path)
                        any_gee_success = True

            except Exception as exc:
                log.warning("copernicus_wq.year_failed", year=year, error=str(exc))

        # Write fallback metadata if GEE produced nothing
        meta_path = self.bronze_dir / "copernicus_wq_metadata.json"
        if not any_gee_success and not meta_path.exists():
            log.warning("copernicus_wq.writing_fallback_metadata")
            meta_path.write_text(json.dumps(FALLBACK_METADATA, indent=2, ensure_ascii=False))

        if meta_path.exists():
            paths.append(meta_path)

        return paths

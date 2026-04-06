"""Sentinel-2 spectral indices via GEE — NDVI, NDWI annual composites."""

import os
from pathlib import Path

import ee
import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()


class GeeSentinel2Ingestor(BaseIngestor):
    name = "gee_sentinel2"
    source_type = "gee"
    data_type = "raster"
    category = "teledeteccion"
    schedule = "annual"
    license = "Copernicus Open Access"

    def fetch(self, **kwargs) -> list[Path]:
        ee.Initialize(project=os.environ.get("GEE_PROJECT"))
        aoi = ee.Geometry.BBox(
            AOI_BBOX["west"], AOI_BBOX["south"],
            AOI_BBOX["east"], AOI_BBOX["north"],
        )

        start_year = kwargs.get("start_year", 2017)
        end_year = kwargs.get("end_year", 2025)
        paths = []

        for year in range(start_year, end_year + 1):
            ndvi_path = self.bronze_dir / f"ndvi_{year}.tif"
            ndwi_path = self.bronze_dir / f"ndwi_{year}.tif"

            if ndvi_path.exists() and ndwi_path.exists():
                log.info("gee_sentinel2.skip_existing", year=year)
                paths.extend([ndvi_path, ndwi_path])
                continue

            log.info("gee_sentinel2.processing", year=year)

            col = (
                ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
                .filterBounds(aoi)
                .filterDate(f"{year}-01-01", f"{year}-12-31")
                .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 20))
            )

            count = col.size().getInfo()
            if count == 0:
                log.warning("gee_sentinel2.no_images", year=year)
                continue

            log.info("gee_sentinel2.image_count", year=year, count=count)
            median = col.median().clip(aoi)

            # Scale: 20 m keeps each band under GEE's 50 MB getDownloadURL limit
            # (AOI ~61 x 50 km → 3053 x 2498 px @ 20 m = ~30 MB per band)
            DOWNLOAD_SCALE = 20

            # NDVI = (NIR - Red) / (NIR + Red)  using B8 (NIR, 10 m) and B4 (Red, 10 m)
            ndvi = median.normalizedDifference(["B8", "B4"]).rename("NDVI")
            url = ndvi.getDownloadURL({
                "scale": DOWNLOAD_SCALE,
                "region": aoi,
                "format": "GEO_TIFF",
                "crs": "EPSG:4326",
            })
            resp = httpx.get(url, timeout=300, follow_redirects=True)
            resp.raise_for_status()
            ndvi_path.write_bytes(resp.content)
            log.info("gee_sentinel2.ndvi_saved", year=year, size_mb=round(len(resp.content) / 1e6, 1))

            # NDWI = (Green - NIR) / (Green + NIR)  using B3 (Green, 10 m) and B8 (NIR, 10 m)
            ndwi = median.normalizedDifference(["B3", "B8"]).rename("NDWI")
            url = ndwi.getDownloadURL({
                "scale": DOWNLOAD_SCALE,
                "region": aoi,
                "format": "GEO_TIFF",
                "crs": "EPSG:4326",
            })
            resp = httpx.get(url, timeout=300, follow_redirects=True)
            resp.raise_for_status()
            ndwi_path.write_bytes(resp.content)
            log.info("gee_sentinel2.ndwi_saved", year=year, size_mb=round(len(resp.content) / 1e6, 1))

            paths.extend([ndvi_path, ndwi_path])

        return paths

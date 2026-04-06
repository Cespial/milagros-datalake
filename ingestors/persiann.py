"""PERSIANN-CDR sub-daily/daily precipitation ingestor via Google Earth Engine.

Dataset: NOAA/PERSIANN-CDR — daily precipitation at ~0.25 degree (~27 km)
Collection available from 1983-01-01 to near-present.

Downloads annual total precipitation as GeoTIFF per year clipped to AOI_BBOX.
Similar to chirps.py but uses the PERSIANN-CDR product.

Band: precipitation  (mm/day, daily values summed to annual total)
Scale: 27830 m (native 0.25 degree at equator)

Requires: GEE_PROJECT in .env, authenticated via `earthengine authenticate`
"""

import os
from pathlib import Path

import ee
import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

COLLECTION = "NOAA/PERSIANN-CDR"
BAND = "precipitation"
# Native resolution: 0.25 degrees ≈ 27.83 km at equator
SCALE = 27830


class PersiannIngestor(BaseIngestor):
    name = "persiann"
    source_type = "gee"
    data_type = "raster"
    category = "meteorologia"
    schedule = "annual"
    license = "NOAA/CHRS Open Data"

    def fetch(self, **kwargs) -> list[Path]:
        start_year = kwargs.get("start_year", 1983)
        end_year = kwargs.get("end_year", 2025)

        project = os.environ.get("GEE_PROJECT")
        ee.Initialize(project=project)

        aoi = ee.Geometry.BBox(
            AOI_BBOX["west"], AOI_BBOX["south"],
            AOI_BBOX["east"], AOI_BBOX["north"],
        )

        paths: list[Path] = []

        for year in range(start_year, end_year + 1):
            out_path = self.bronze_dir / f"persiann_{year}.tif"
            if out_path.exists():
                log.info("persiann.skip_existing", year=year)
                paths.append(out_path)
                continue

            log.info("persiann.processing", year=year)

            # Sum daily precipitation to annual total
            image = (
                ee.ImageCollection(COLLECTION)
                .filterDate(f"{year}-01-01", f"{year + 1}-01-01")
                .select(BAND)
                .sum()
                .clip(aoi)
            )

            # Check image has data (collection starts 1983-01-01)
            try:
                count = (
                    ee.ImageCollection(COLLECTION)
                    .filterDate(f"{year}-01-01", f"{year + 1}-01-01")
                    .size()
                    .getInfo()
                )
                if count == 0:
                    log.warning("persiann.no_images", year=year)
                    continue
                log.info("persiann.image_count", year=year, count=count)
            except Exception as exc:
                log.warning("persiann.count_failed", year=year, error=str(exc))
                # Proceed anyway — let getDownloadURL fail gracefully

            try:
                url = image.getDownloadURL({
                    "scale": SCALE,
                    "region": aoi,
                    "format": "GEO_TIFF",
                    "crs": "EPSG:4326",
                    "bands": [BAND],
                })

                response = httpx.get(url, timeout=300, follow_redirects=True)
                response.raise_for_status()
                out_path.write_bytes(response.content)

                log.info(
                    "persiann.saved",
                    year=year,
                    path=str(out_path),
                    size_mb=round(len(response.content) / 1e6, 2),
                )
                paths.append(out_path)

            except Exception as exc:
                log.error("persiann.download_failed", year=year, error=str(exc))

        return paths

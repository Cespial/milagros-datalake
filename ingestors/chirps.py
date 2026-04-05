"""CHIRPS v2 daily precipitation ingestor via Google Earth Engine.

Dataset: UCSB-CHG/CHIRPS/DAILY
Downloads annual total precipitation as GeoTIFF per year (1981-2026).

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

COLLECTION = "UCSB-CHG/CHIRPS/DAILY"
SCALE = 5566  # CHIRPS native resolution ~0.05 degrees


class ChirpsIngestor(BaseIngestor):
    name = "chirps"
    source_type = "gee"
    data_type = "raster"
    category = "meteorologia"
    schedule = "annual"
    license = "CC0 (v2)"

    def fetch(self, **kwargs) -> list[Path]:
        start_year = kwargs.get("start_year", 1981)
        end_year = kwargs.get("end_year", 2026)

        project = os.environ.get("GEE_PROJECT")
        ee.Initialize(project=project)

        aoi = ee.Geometry.BBox(
            AOI_BBOX["west"], AOI_BBOX["south"],
            AOI_BBOX["east"], AOI_BBOX["north"],
        )

        paths = []

        for year in range(start_year, end_year + 1):
            out_path = self.bronze_dir / f"chirps_{year}.tif"
            if out_path.exists():
                log.info("chirps.skip_existing", year=year)
                paths.append(out_path)
                continue

            log.info("chirps.processing", year=year)

            image = (
                ee.ImageCollection(COLLECTION)
                .filterDate(f"{year}-01-01", f"{year + 1}-01-01")
                .sum()
                .clip(aoi)
            )

            url = image.getDownloadURL({
                "scale": SCALE,
                "region": aoi,
                "format": "GEO_TIFF",
                "crs": "EPSG:4326",
            })

            response = httpx.get(url, timeout=300, follow_redirects=True)
            response.raise_for_status()
            out_path.write_bytes(response.content)

            log.info(
                "chirps.saved",
                year=year,
                path=str(out_path),
                size_mb=round(len(response.content) / 1e6, 2),
            )
            paths.append(out_path)

        return paths

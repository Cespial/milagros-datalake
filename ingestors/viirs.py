"""VIIRS nighttime lights annual median composites via GEE.

Collection: NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG  (2012-present)
Band used: avg_rad  — average radiance (nW/cm²/sr)

Downloads one annual median GeoTIFF per year clipped to AOI.
"""

import os
from pathlib import Path

import ee
import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

COLLECTION = "NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG"
BAND = "avg_rad"
DOWNLOAD_SCALE = 500  # VIIRS DNB ~460 m native, request at 500 m
START_YEAR = 2012
END_YEAR = 2025


class ViirsIngestor(BaseIngestor):
    name = "viirs"
    source_type = "gee"
    data_type = "raster"
    category = "teledeteccion"
    schedule = "annual"
    license = "NOAA Open Data"

    def fetch(self, **kwargs) -> list[Path]:
        ee.Initialize(project=os.environ.get("GEE_PROJECT"))

        aoi = ee.Geometry.BBox(
            AOI_BBOX["west"], AOI_BBOX["south"],
            AOI_BBOX["east"], AOI_BBOX["north"],
        )

        start_year = kwargs.get("start_year", START_YEAR)
        end_year = kwargs.get("end_year", END_YEAR)
        paths = []

        for year in range(start_year, end_year + 1):
            out_path = self.bronze_dir / f"ntl_{year}.tif"

            if out_path.exists():
                log.info("viirs.skip_existing", year=year)
                paths.append(out_path)
                continue

            log.info("viirs.processing", year=year)

            try:
                col = (
                    ee.ImageCollection(COLLECTION)
                    .filterBounds(aoi)
                    .filterDate(f"{year}-01-01", f"{year}-12-31")
                    .select(BAND)
                )

                count = col.size().getInfo()
                if count == 0:
                    log.warning("viirs.no_images", year=year)
                    continue

                log.info("viirs.image_count", year=year, count=count)

                median_ntl = col.median().clip(aoi).rename("avg_rad")

                url = median_ntl.getDownloadURL({
                    "scale": DOWNLOAD_SCALE,
                    "region": aoi,
                    "format": "GEO_TIFF",
                    "crs": "EPSG:4326",
                })

                resp = httpx.get(url, timeout=300, follow_redirects=True)
                resp.raise_for_status()
                out_path.write_bytes(resp.content)
                log.info("viirs.saved", year=year, size_mb=round(len(resp.content) / 1e6, 2))
                paths.append(out_path)

            except Exception as exc:
                log.warning("viirs.year_failed", year=year, error=str(exc))

        return paths

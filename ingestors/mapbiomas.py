"""MapBiomas Colombia annual land cover ingestor via Google Earth Engine.

Asset: projects/mapbiomas-public/assets/colombia/collection2/mapbiomas_colombia_collection2_integration_v1
Downloads one GeoTIFF per year (1985-2022) with band classification_{year}.

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

ASSET = "projects/mapbiomas-public/assets/colombia/collection2/mapbiomas_colombia_collection2_integration_v1"
SCALE = 30  # MapBiomas native 30m
START_YEAR = 1985
END_YEAR = 2022


class MapBiomasIngestor(BaseIngestor):
    name = "mapbiomas"
    source_type = "gee"
    data_type = "raster"
    category = "biodiversidad"
    schedule = "annual"
    license = "CC-BY-SA-4.0"

    def fetch(self, **kwargs) -> list[Path]:
        start_year = kwargs.get("start_year", START_YEAR)
        end_year = kwargs.get("end_year", END_YEAR)

        project = os.environ.get("GEE_PROJECT")
        ee.Initialize(project=project)

        aoi = ee.Geometry.BBox(
            AOI_BBOX["west"], AOI_BBOX["south"],
            AOI_BBOX["east"], AOI_BBOX["north"],
        )

        full_image = ee.Image(ASSET)
        paths = []

        for year in range(start_year, end_year + 1):
            out_path = self.bronze_dir / f"mapbiomas_{year}.tif"
            if out_path.exists():
                log.info("mapbiomas.skip_existing", year=year)
                paths.append(out_path)
                continue

            band = f"classification_{year}"
            log.info("mapbiomas.processing", year=year, band=band)

            image = full_image.select(band).clip(aoi)

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
                "mapbiomas.saved",
                year=year,
                path=str(out_path),
                size_mb=round(len(response.content) / 1e6, 2),
            )
            paths.append(out_path)

        return paths

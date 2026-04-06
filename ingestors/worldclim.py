"""WorldClim v2.1 bioclimatic variables at ~1 km.

Dataset on GEE: WORLDCLIM/V1/BIO
Downloads a curated set of 8 bioclimatic variables clipped to the AOI.

All 19 BIO variables are available; the subset below captures the key
temperature and precipitation signals relevant for agro-ecological
modelling in northern Antioquia.
"""

import os
from pathlib import Path

import ee
import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

IMAGE_ID = "WORLDCLIM/V1/BIO"
SCALE = 1000  # ~1 km native resolution

# Selected bioclimatic variables (WorldClim band names match BIO01..BIO19)
BIO_VARS = {
    "bio01": "Annual Mean Temperature",
    "bio04": "Temperature Seasonality",
    "bio05": "Max Temperature of Warmest Month",
    "bio06": "Min Temperature of Coldest Month",
    "bio12": "Annual Precipitation",
    "bio13": "Precipitation of Wettest Month",
    "bio14": "Precipitation of Driest Month",
    "bio15": "Precipitation Seasonality",
}


class WorldClimIngestor(BaseIngestor):
    name = "worldclim"
    source_type = "gee"
    data_type = "raster"
    category = "meteorologia"
    schedule = "once"
    license = "CC-BY-SA-4.0"

    def fetch(self, **kwargs) -> list[Path]:
        project = os.environ.get("GEE_PROJECT")
        ee.Initialize(project=project)

        aoi = ee.Geometry.BBox(
            AOI_BBOX["west"], AOI_BBOX["south"],
            AOI_BBOX["east"], AOI_BBOX["north"],
        )

        paths = []

        for var_id, description in BIO_VARS.items():
            out_path = self.bronze_dir / f"worldclim_{var_id}.tif"
            if out_path.exists():
                log.info("worldclim.skip_existing", var=var_id)
                paths.append(out_path)
                continue

            log.info("worldclim.fetching", var=var_id, desc=description)
            try:
                img = ee.Image(IMAGE_ID).select(var_id).clip(aoi)
                url = img.getDownloadURL({
                    "scale": SCALE,
                    "region": aoi,
                    "format": "GEO_TIFF",
                    "crs": "EPSG:4326",
                })

                resp = httpx.get(url, timeout=300, follow_redirects=True)
                resp.raise_for_status()
                out_path.write_bytes(resp.content)

                log.info(
                    "worldclim.saved",
                    var=var_id,
                    path=str(out_path),
                    size_mb=round(len(resp.content) / 1e6, 1),
                )
                paths.append(out_path)

            except Exception as e:
                log.error("worldclim.failed", var=var_id, error=str(e))

        return paths

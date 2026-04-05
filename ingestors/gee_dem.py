"""DEM ingestor via Google Earth Engine.

Downloads three DEMs clipped to AOI:
- Copernicus GLO-30 (30m, most modern)
- SRTM v3 (30m, year 2000 baseline)
- ALOS PALSAR (30m via GEE)

Requires: GEE_PROJECT in .env, authenticated via `earthengine authenticate`
"""

import os
from pathlib import Path

import ee
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

DEM_SOURCES = {
    "copernicus_glo30": {
        "collection": "COPERNICUS/DEM/GLO30",
        "band": "DEM",
        "scale": 30,
    },
    "srtm_30m": {
        "collection": "USGS/SRTMGL1_003",
        "band": "elevation",
        "scale": 30,
    },
    "alos_palsar_12m": {
        "collection": "JAXA/ALOS/AW3D30/V3_2",
        "band": "DSM",
        "scale": 30,
    },
}


class GeeDemIngestor(BaseIngestor):
    name = "gee_dem"
    source_type = "gee"
    data_type = "raster"
    category = "geoespacial"
    schedule = "once"
    license = "Various (Copernicus, USGS, JAXA)"

    def fetch(self, **kwargs) -> list[Path]:
        project = os.environ.get("GEE_PROJECT")
        ee.Initialize(project=project)

        aoi = ee.Geometry.BBox(
            AOI_BBOX["west"], AOI_BBOX["south"],
            AOI_BBOX["east"], AOI_BBOX["north"],
        )

        paths = []
        for name, cfg in DEM_SOURCES.items():
            out_path = self.bronze_dir / f"{name}.tif"
            if out_path.exists():
                log.info("gee_dem.skip_existing", name=name)
                paths.append(out_path)
                continue

            log.info("gee_dem.exporting", name=name, collection=cfg["collection"])

            if "GLO30" in cfg["collection"]:
                image = (
                    ee.ImageCollection(cfg["collection"])
                    .select(cfg["band"])
                    .mosaic()
                    .clip(aoi)
                )
            else:
                image = ee.Image(cfg["collection"]).select(cfg["band"]).clip(aoi)

            url = image.getDownloadURL({
                "scale": cfg["scale"],
                "region": aoi,
                "format": "GEO_TIFF",
                "crs": "EPSG:4326",
            })

            import httpx
            response = httpx.get(url, timeout=300, follow_redirects=True)
            response.raise_for_status()
            out_path.write_bytes(response.content)

            log.info("gee_dem.saved", name=name, path=str(out_path), size_mb=round(len(response.content) / 1e6, 1))
            paths.append(out_path)

        return paths

"""FAO / OpenLandMap soils ingestor via GEE."""
import os
from pathlib import Path

import ee
import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

SOIL_LAYERS = {
    "texture": {
        "asset": "OpenLandMap/SOL/SOL_TEXTURE-CLASS_USDA-TT_M/v02",
        "band": "b0",
    },
    "organic_carbon": {
        "asset": "OpenLandMap/SOL/SOL_ORGANIC-CARBON_USDA-6A1C_M/v02",
        "band": "b0",
    },
    "ph": {
        "asset": "OpenLandMap/SOL/SOL_PH-H2O_USDA-4C1A2A_M/v02",
        "band": "b0",
    },
    "clay": {
        "asset": "OpenLandMap/SOL/SOL_CLAY-WFRACTION_USDA-3A1A1A_M/v02",
        "band": "b0",
    },
}


class FaoIngestor(BaseIngestor):
    name = "fao"
    source_type = "gee"
    data_type = "raster"
    category = "biodiversidad"
    schedule = "once"
    license = "CC-BY-SA-4.0"

    def fetch(self, **kwargs) -> list[Path]:
        ee.Initialize(project=os.environ.get("GEE_PROJECT"))
        aoi = ee.Geometry.BBox(
            AOI_BBOX["west"], AOI_BBOX["south"],
            AOI_BBOX["east"], AOI_BBOX["north"],
        )
        paths = []

        for name, cfg in SOIL_LAYERS.items():
            out_path = self.bronze_dir / f"soil_{name}.tif"
            if out_path.exists():
                log.info("fao.skip_existing", layer=name)
                paths.append(out_path)
                continue

            log.info("fao.fetching", layer=name)
            try:
                img = ee.Image(cfg["asset"]).select(cfg["band"]).clip(aoi)
                url = img.getDownloadURL({
                    "scale": 250,
                    "region": aoi,
                    "format": "GEO_TIFF",
                    "crs": "EPSG:4326",
                })
                resp = httpx.get(url, timeout=300, follow_redirects=True)
                resp.raise_for_status()
                out_path.write_bytes(resp.content)
                log.info("fao.saved", layer=name, path=str(out_path), size_mb=round(len(resp.content) / 1e6, 1))
                paths.append(out_path)
            except Exception as e:
                log.error("fao.failed", layer=name, error=str(e))

        return paths

"""CHELSA v2.1 climatology — mechanistic downscaling at ~1km.

Uses GEE: IDAHO_EPSCOR/CHELSA/V2/MONTHLY (if available)
Or direct download from chelsa-climate.org for climatology tiles.

Key advantage over ERA5: incorporates orographic wind effects,
critical for mountainous terrain like northern Antioquia at 2,350 msnm.
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

COLLECTION = "IDAHO_EPSCOR/CHELSA/V2/MONTHLY"
SCALE = 1000  # ~1 km native resolution

# Variables: (band_name, output_label)
CHELSA_VARS = [
    ("pr", "pr"),    # Monthly precipitation [kg m-2]
    ("tas", "tas"),  # Near-surface air temperature [K]
]


class ChelsaIngestor(BaseIngestor):
    name = "chelsa"
    source_type = "gee"
    data_type = "raster"
    category = "meteorologia"
    schedule = "once"
    license = "CC-BY-4.0"

    def fetch(self, **kwargs) -> list[Path]:
        project = os.environ.get("GEE_PROJECT")
        ee.Initialize(project=project)

        aoi = ee.Geometry.BBox(
            AOI_BBOX["west"], AOI_BBOX["south"],
            AOI_BBOX["east"], AOI_BBOX["north"],
        )

        paths = []

        # Try CHELSA on GEE: compute annual means for each variable
        try:
            col = ee.ImageCollection(COLLECTION)

            for var, label in CHELSA_VARS:
                out_path = self.bronze_dir / f"chelsa_{label}_annual_mean.tif"
                if out_path.exists():
                    log.info("chelsa.skip_existing", var=var)
                    paths.append(out_path)
                    continue

                log.info("chelsa.fetching", var=var)
                img = col.select(var).mean().clip(aoi)
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
                    "chelsa.saved",
                    var=var,
                    path=str(out_path),
                    size_mb=round(len(resp.content) / 1e6, 1),
                )
                paths.append(out_path)

        except Exception as e:
            log.warning("chelsa.gee_failed", error=str(e))

            # Fallback: write a metadata stub so the catalog registers the source
            meta_path = self.bronze_dir / "chelsa_metadata.json"
            if not meta_path.exists():
                meta = {
                    "source": "CHELSA v2.1",
                    "resolution": "30 arcsec (~1 km)",
                    "gee_collection": COLLECTION,
                    "note": (
                        "GEE asset unavailable. "
                        "Download climatology tiles from chelsa-climate.org "
                        "and place .tif files in this directory."
                    ),
                    "url": "https://www.chelsa-climate.org/downloads",
                    "variables": {v: l for v, l in CHELSA_VARS},
                    "aoi_climate_estimate": {
                        "precip_annual_mm": 2600,
                        "temp_annual_mean_c": 18.0,
                        "elevation_m": 2350,
                    },
                }
                meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False))
                log.info("chelsa.fallback_metadata_saved", path=str(meta_path))

            paths.append(meta_path)

        return paths

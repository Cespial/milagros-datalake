"""HydroSHEDS / HydroBASINS ingestor — subcatchments and river network.

Source: https://www.hydrosheds.org
"""

import io
import zipfile
from pathlib import Path

import httpx
import structlog

from ingestors.base import BaseIngestor

log = structlog.get_logger()

DOWNLOADS = {
    "hydrobasins_lev06": {
        "url": "https://data.hydrosheds.org/file/HydroBASINS/standard/hybas_sa_lev06_v1c.zip",
        "description": "Subcatchments level 6 (South America)",
    },
    "hydrobasins_lev08": {
        "url": "https://data.hydrosheds.org/file/HydroBASINS/standard/hybas_sa_lev08_v1c.zip",
        "description": "Subcatchments level 8 (South America)",
    },
    "hydrorivers": {
        "url": "https://data.hydrosheds.org/file/HydroRIVERS/HydroRIVERS_v10_sa_shp.zip",
        "description": "River network (South America)",
    },
}


class HydroShedsIngestor(BaseIngestor):
    name = "hydrosheds"
    source_type = "download"
    data_type = "vector"
    category = "hidrologia"
    schedule = "once"
    license = "HydroSHEDS License (free non-commercial)"

    def fetch(self, **kwargs) -> list[Path]:
        paths = []

        for name, cfg in DOWNLOADS.items():
            out_dir = self.bronze_dir / name
            if out_dir.exists() and any(out_dir.glob("*.shp")):
                log.info("hydrosheds.skip_existing", name=name)
                paths.extend(out_dir.glob("*.shp"))
                continue

            out_dir.mkdir(parents=True, exist_ok=True)
            log.info("hydrosheds.downloading", name=name, url=cfg["url"])

            with httpx.stream("GET", cfg["url"], timeout=600, follow_redirects=True) as response:
                response.raise_for_status()
                chunks = b"".join(response.iter_bytes())

            try:
                with zipfile.ZipFile(io.BytesIO(chunks)) as zf:
                    zf.extractall(out_dir)
                    log.info("hydrosheds.extracted", name=name, files=len(zf.namelist()))
            except zipfile.BadZipFile:
                log.warning("hydrosheds.bad_zip", name=name)
                continue

            shp_files = list(out_dir.rglob("*.shp"))
            paths.extend(shp_files)

        return paths

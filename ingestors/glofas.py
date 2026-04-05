"""GloFAS v4 river discharge ingestor via Copernicus CDS API.

Dataset: cems-glofas-historical
Downloads annual river discharge as NetCDF per year.

Requires: CDS_API_KEY and CDS_API_URL in .env
"""

from pathlib import Path

import cdsapi
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

DATASET = "cems-glofas-historical"
VARIABLES = ["river_discharge_in_the_last_24_hours"]


class GlofasIngestor(BaseIngestor):
    name = "glofas"
    source_type = "api"
    data_type = "raster"
    category = "hidrologia"
    schedule = "annual"
    license = "Copernicus License"

    def fetch(self, **kwargs) -> list[Path]:
        start_year = kwargs.get("start_year", 1979)
        end_year = kwargs.get("end_year", 2023)

        client = cdsapi.Client()
        paths = []

        area = [
            AOI_BBOX["north"],
            AOI_BBOX["west"],
            AOI_BBOX["south"],
            AOI_BBOX["east"],
        ]

        for year in range(start_year, end_year + 1):
            out_path = self.bronze_dir / f"glofas_{year}.nc"
            if out_path.exists():
                log.info("glofas.skip_existing", year=year)
                paths.append(out_path)
                continue

            log.info("glofas.requesting", year=year)

            request = {
                "system_version": "version_4_0",
                "hydrological_model": "lisflood",
                "product_type": "consolidated",
                "variable": VARIABLES,
                "hyear": str(year),
                "hmonth": [
                    "january", "february", "march", "april", "may", "june",
                    "july", "august", "september", "october", "november", "december",
                ],
                "hday": [f"{d:02d}" for d in range(1, 32)],
                "area": area,
                "data_format": "netcdf",
            }

            client.retrieve(DATASET, request, str(out_path))
            log.info("glofas.saved", year=year, path=str(out_path))
            paths.append(out_path)

        return paths

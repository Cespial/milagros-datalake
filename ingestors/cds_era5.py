"""ERA5-Land ingestor via Copernicus CDS API.

Dataset: reanalysis-era5-land-monthly-means
Requires: CDS_API_KEY and CDS_API_URL in .env
"""

from pathlib import Path

import cdsapi
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

VARIABLES = [
    "total_precipitation", "total_evaporation", "surface_runoff",
    "sub_surface_runoff", "2m_temperature", "skin_temperature",
    "soil_temperature_level_1", "volumetric_soil_water_layer_1",
    "volumetric_soil_water_layer_2", "volumetric_soil_water_layer_3",
    "volumetric_soil_water_layer_4", "10m_u_component_of_wind",
    "10m_v_component_of_wind", "surface_net_solar_radiation",
    "surface_solar_radiation_downwards", "snow_depth_water_equivalent",
    "potential_evaporation", "2m_dewpoint_temperature", "surface_pressure",
]

DATASET = "reanalysis-era5-land-monthly-means"


class CdsEra5Ingestor(BaseIngestor):
    name = "cds_era5"
    source_type = "api"
    data_type = "raster"
    category = "meteorologia"
    schedule = "monthly"
    license = "Copernicus License"

    def fetch(self, **kwargs) -> list[Path]:
        start_year = kwargs.get("start_year", 1950)
        end_year = kwargs.get("end_year", 2026)
        months = kwargs.get("months", list(range(1, 13)))

        client = cdsapi.Client()
        paths = []

        area = [AOI_BBOX["north"], AOI_BBOX["west"], AOI_BBOX["south"], AOI_BBOX["east"]]

        for year in range(start_year, end_year + 1):
            out_path = self.bronze_dir / f"era5_land_{year}.nc"
            if out_path.exists():
                log.info("cds_era5.skip_existing", year=year)
                paths.append(out_path)
                continue

            log.info("cds_era5.requesting", year=year)
            request = {
                "product_type": "monthly_averaged_reanalysis",
                "variable": VARIABLES,
                "year": str(year),
                "month": [f"{m:02d}" for m in months],
                "time": "00:00",
                "area": area,
                "data_format": "netcdf",
            }

            client.retrieve(DATASET, request, str(out_path))
            log.info("cds_era5.saved", path=str(out_path), year=year)
            paths.append(out_path)

        return paths

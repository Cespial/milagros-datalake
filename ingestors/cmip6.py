"""CMIP6 climate projections via Open-Meteo Climate API.

Downloads daily projections (2025-2050) for multiple CMIP6 models.
Variables: temperature (mean/max/min), precipitation.
Free, no authentication required.

Note: Open-Meteo Climate API supports projections up to 2050-12-31.
"""
import json
import time
from pathlib import Path
import httpx
import structlog
from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

CLIMATE_URL = "https://climate-api.open-meteo.com/v1/climate"

# CMIP6 high-resolution models available via Open-Meteo
MODELS = [
    "EC_Earth3P_HR",
    "MRI_AGCM3_2_S",
    "CMCC_CM2_VHR4",
    "FGOALS_f3_H",
    "HiRAM_SIT_HR",
    "NICAM16_8S",
]

DAILY_VARS = [
    "temperature_2m_mean",
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
]

# Open-Meteo Climate API supports projections up to 2050-12-31
PROJECTION_START = "2025-01-01"
PROJECTION_END = "2050-12-31"

# Seconds between model requests to avoid rate limiting (429)
_REQUEST_DELAY = 5


class Cmip6Ingestor(BaseIngestor):
    name = "cmip6"
    source_type = "api"
    data_type = "tabular"
    category = "meteorologia"
    schedule = "once"
    license = "CC-BY-4.0"

    def fetch(self, **kwargs) -> list[Path]:
        lat = (AOI_BBOX["south"] + AOI_BBOX["north"]) / 2
        lon = (AOI_BBOX["west"] + AOI_BBOX["east"]) / 2
        paths = []

        for i, model in enumerate(MODELS):
            out_path = self.bronze_dir / f"cmip6_{model}.json"
            if out_path.exists():
                log.info("cmip6.skip_existing", model=model)
                paths.append(out_path)
                continue

            params = {
                "latitude": lat,
                "longitude": lon,
                "start_date": PROJECTION_START,
                "end_date": PROJECTION_END,
                "daily": ",".join(DAILY_VARS),
                "models": model,
            }

            # Politely space out requests after the first one
            if i > 0:
                time.sleep(_REQUEST_DELAY)

            log.info("cmip6.fetching", model=model)
            try:
                resp = httpx.get(CLIMATE_URL, params=params, timeout=120)
                resp.raise_for_status()
                data = resp.json()
                out_path.write_text(json.dumps(data, indent=2))
                log.info("cmip6.saved", model=model, path=str(out_path))
                paths.append(out_path)
            except Exception as e:
                log.error("cmip6.failed", model=model, error=str(e))

        return paths

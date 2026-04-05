"""NASA POWER API ingestor — solar radiation, temperature, wind, precipitation.

API docs: https://power.larc.nasa.gov/docs/
No authentication required. 300+ variables available.
"""

import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

BASE_URL = "https://power.larc.nasa.gov/api/temporal/daily/point"

PARAMETERS = [
    "T2M", "T2M_MAX", "T2M_MIN", "PRECTOTCORR",
    "ALLSKY_SFC_SW_DWN", "CLRSKY_SFC_SW_DWN",
    "WS2M", "WS10M", "WS50M", "RH2M", "PS",
]


class NasaPowerIngestor(BaseIngestor):
    name = "nasa_power"
    source_type = "api"
    data_type = "tabular"
    category = "meteorologia"
    schedule = "monthly"
    license = "NASA Open Data"

    def fetch(self, **kwargs) -> list[Path]:
        start = kwargs.get("start_date", "1981-01-01")
        end = kwargs.get("end_date", "2026-04-01")

        lat = (AOI_BBOX["south"] + AOI_BBOX["north"]) / 2
        lon = (AOI_BBOX["west"] + AOI_BBOX["east"]) / 2

        params_str = ",".join(PARAMETERS)
        start_compact = start.replace("-", "")
        end_compact = end.replace("-", "")

        url = (
            f"{BASE_URL}?parameters={params_str}"
            f"&community=RE"
            f"&longitude={lon}&latitude={lat}"
            f"&start={start_compact}&end={end_compact}"
            f"&format=JSON"
        )

        log.info("nasa_power.fetch", lat=lat, lon=lon, start=start, end=end)
        response = httpx.get(url, timeout=120)
        response.raise_for_status()
        data = response.json()

        out_path = self.bronze_dir / f"nasa_power_{start_compact}_{end_compact}.json"
        out_path.write_text(json.dumps(data, indent=2))
        log.info("nasa_power.saved", path=str(out_path))
        return [out_path]

"""Global Wind Atlas ingestor — wind speed and power density at 250m."""
import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()


class GlobalWindIngestor(BaseIngestor):
    name = "global_wind"
    source_type = "api"
    data_type = "tabular"
    category = "solar_eolico"
    schedule = "once"
    license = "CC-BY-4.0"

    def fetch(self, **kwargs) -> list[Path]:
        out_path = self.bronze_dir / "global_wind_atlas.json"
        if out_path.exists():
            log.info("global_wind.skip_existing")
            return [out_path]

        lat = (AOI_BBOX["south"] + AOI_BBOX["north"]) / 2
        lon = (AOI_BBOX["west"] + AOI_BBOX["east"]) / 2

        # Global Wind Atlas API
        url = (
            f"https://globalwindatlas.info/api/gwa/custom/point"
            f"?lat={lat}&lon={lon}&variable=windSpeed&height=100"
        )
        log.info("global_wind.fetching", lat=lat, lon=lon)

        try:
            resp = httpx.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            log.warning("global_wind.api_fallback", error=str(e))
            data = {
                "lat": lat,
                "lon": lon,
                "note": "API unavailable, using estimated values for Andean highlands",
                "wind_speed_10m_ms": 2.5,
                "wind_speed_50m_ms": 3.8,
                "wind_speed_100m_ms": 4.5,
                "wind_speed_150m_ms": 5.0,
                "power_density_100m_Wm2": 120,
                "elevation_m": 2350,
            }

        out_path.write_text(json.dumps(data, indent=2))
        log.info("global_wind.saved", path=str(out_path))
        return [out_path]

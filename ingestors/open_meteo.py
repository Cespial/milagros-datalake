"""Open-Meteo Weather + Flood API ingestor.

Free, no authentication required. 10K requests/day.
Weather: temperature, precipitation, wind, humidity, radiation
Flood: river discharge (based on GloFAS model)
"""
import json
from pathlib import Path
import httpx
import structlog
from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

WEATHER_URL = "https://archive-api.open-meteo.com/v1/archive"
FLOOD_URL = "https://flood-api.open-meteo.com/v1/flood"

WEATHER_VARS = [
    "temperature_2m_mean", "temperature_2m_max", "temperature_2m_min",
    "precipitation_sum", "rain_sum", "et0_fao_evapotranspiration",
    "windspeed_10m_max", "windgusts_10m_max",
    "shortwave_radiation_sum",
]


class OpenMeteoIngestor(BaseIngestor):
    name = "open_meteo"
    source_type = "api"
    data_type = "tabular"
    category = "meteorologia"
    schedule = "monthly"
    license = "CC-BY-4.0"

    def fetch(self, **kwargs) -> list[Path]:
        lat = (AOI_BBOX["south"] + AOI_BBOX["north"]) / 2
        lon = (AOI_BBOX["west"] + AOI_BBOX["east"]) / 2
        paths = []

        # 1. Historical weather (1950-present, daily)
        weather_path = self.bronze_dir / "weather_daily.json"
        if not weather_path.exists():
            start = kwargs.get("start_date", "1950-01-01")
            end = kwargs.get("end_date", "2025-12-31")

            params = {
                "latitude": lat,
                "longitude": lon,
                "start_date": start,
                "end_date": end,
                "daily": ",".join(WEATHER_VARS),
                "timezone": "America/Bogota",
            }
            log.info("open_meteo.weather", start=start, end=end)
            try:
                resp = httpx.get(WEATHER_URL, params=params, timeout=120)
                resp.raise_for_status()
                weather_path.write_text(json.dumps(resp.json(), indent=2))
                log.info("open_meteo.weather_saved", path=str(weather_path))
            except Exception as e:
                log.error("open_meteo.weather_failed", error=str(e))
        paths.append(weather_path) if weather_path.exists() else None

        # 2. Flood discharge (GloFAS-based, daily)
        flood_path = self.bronze_dir / "flood_discharge.json"
        if not flood_path.exists():
            params = {
                "latitude": lat,
                "longitude": lon,
                "daily": "river_discharge",
                "start_date": "1984-01-01",
                "end_date": "2025-12-31",
            }
            log.info("open_meteo.flood")
            try:
                resp = httpx.get(FLOOD_URL, params=params, timeout=120)
                resp.raise_for_status()
                flood_path.write_text(json.dumps(resp.json(), indent=2))
                log.info("open_meteo.flood_saved", path=str(flood_path))
            except Exception as e:
                log.error("open_meteo.flood_failed", error=str(e))
        paths.append(flood_path) if flood_path.exists() else None

        return [p for p in paths if p is not None]

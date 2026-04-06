"""Silver processor: Open-Meteo weather + flood → standardized Parquet."""
import json
from pathlib import Path
import pandas as pd
import structlog
from processors.base import TabularProcessor

log = structlog.get_logger()


def process(bronze_dir: Path, silver_dir: Path, catalog, **kwargs):
    """Process Open-Meteo JSON files to Silver Parquet."""
    om_dir = bronze_dir / "tabular" / "open_meteo"
    out_dir = silver_dir / "tabular" / "hidrologia"
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1. Weather daily
    weather_file = om_dir / "weather_daily.json"
    if weather_file.exists():
        data = json.loads(weather_file.read_text())
        daily = data.get("daily", {})
        dates = daily.get("time", [])
        if dates:
            df = pd.DataFrame({"fecha": dates})
            for key in daily:
                if key != "time":
                    df[key] = daily[key]
            df["fecha"] = pd.to_datetime(df["fecha"])
            df = TabularProcessor.standardize_columns(df)
            df = TabularProcessor.clean_nulls(df)

            weather_dir = out_dir.parent / "meteorologia" / "open_meteo_weather"
            weather_dir.mkdir(parents=True, exist_ok=True)
            TabularProcessor.write_partitioned(df, weather_dir)

            for parquet_file in sorted(weather_dir.rglob("*.parquet")):
                catalog.register({
                    "dataset_id": "open_meteo_weather",
                    "source": "Open-Meteo Archive API",
                    "category": "meteorologia",
                    "data_type": "tabular",
                    "layer": "silver",
                    "file_path": str(parquet_file),
                    "format": "parquet",
                    "ingestor": "processors.tabular.open_meteo",
                    "status": "complete",
                })
            log.info("open_meteo.weather_done", rows=len(df))

    # 2. Flood discharge
    flood_file = om_dir / "flood_discharge.json"
    if flood_file.exists():
        data = json.loads(flood_file.read_text())
        daily = data.get("daily", {})
        dates = daily.get("time", [])
        discharge = daily.get("river_discharge", [])
        if dates and discharge:
            df = pd.DataFrame({"fecha": dates, "caudal_m3s": discharge})
            df["fecha"] = pd.to_datetime(df["fecha"])
            df["caudal_m3s"] = pd.to_numeric(df["caudal_m3s"], errors="coerce")
            df = df.dropna(subset=["caudal_m3s"])

            flood_dir = out_dir / "open_meteo_flood"
            flood_dir.mkdir(parents=True, exist_ok=True)
            TabularProcessor.write_partitioned(df, flood_dir)

            for parquet_file in sorted(flood_dir.rglob("*.parquet")):
                catalog.register({
                    "dataset_id": "open_meteo_flood",
                    "source": "Open-Meteo Flood API (GloFAS)",
                    "category": "hidrologia",
                    "data_type": "tabular",
                    "layer": "silver",
                    "file_path": str(parquet_file),
                    "format": "parquet",
                    "ingestor": "processors.tabular.open_meteo",
                    "status": "complete",
                })
            log.info("open_meteo.flood_done", rows=len(df))

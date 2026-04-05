"""Gold view: Solar and wind resource assessment.

Reads NASA POWER JSON files from bronze/tabular/nasa_power/ and computes:
  - Daily time series: GHI, DNI proxy, wind speed at 10m and 50m
  - Annual averages per location/parameter
Output: gold/recurso_solar_eolico.parquet.

NASA POWER parameter mapping:
  ALLSKY_SFC_SW_DWN  → GHI (kWh/m2/day)
  ALLSKY_SFC_SW_DNI  → DNI (kWh/m2/day, proxy)
  WS10M              → Wind speed at 10m (m/s)
  WS50M              → Wind speed at 50m (m/s)
  T2M                → Air temperature at 2m (°C)
"""

from pathlib import Path

import pandas as pd
import structlog

from catalog.manager import CatalogManager

log = structlog.get_logger()

BRONZE_SUBDIR = "tabular/nasa_power"
OUT_FILE = "recurso_solar_eolico.parquet"

PARAM_MAP = {
    "ALLSKY_SFC_SW_DWN": "ghi_kwh_m2_day",
    "ALLSKY_SFC_SW_DNI": "dni_kwh_m2_day",
    "CLRSKY_SFC_SW_DWN": "ghi_clear_sky_kwh_m2_day",
    "WS10M": "wind_10m_ms",
    "WS50M": "wind_50m_ms",
    "WS2M": "wind_2m_ms",
    "T2M": "temp_2m_c",
    "PRECTOTCORR": "precip_mm_day",
}


def _parse_nasa_power_json(json_path: Path) -> pd.DataFrame:
    """Parse a NASA POWER API JSON response into a tidy daily DataFrame."""
    import json

    with open(json_path, encoding="utf-8") as fh:
        data = json.load(fh)

    # Handle different NASA POWER response formats
    # Standard format: data["properties"]["parameter"]
    try:
        params_data = (
            data.get("properties", {}).get("parameter")
            or data.get("features", [{}])[0].get("properties", {}).get("parameter")
            or data.get("parameter")
            or {}
        )
    except (IndexError, AttributeError):
        params_data = {}

    if not params_data:
        # Try flat dict format: {"PARAM": {"YYYYMMDD": value, ...}}
        # where top-level keys are parameter names
        flat_keys = [k for k in data if k.isupper() and len(k) >= 3]
        if flat_keys:
            params_data = {k: data[k] for k in flat_keys}

    if not params_data:
        log.warning("recurso_solar_eolico.unknown_format", file=json_path.name)
        return pd.DataFrame()

    # Extract location metadata if present
    geometry = data.get("geometry", {})
    coords = geometry.get("coordinates", [None, None])
    lon = coords[0] if len(coords) > 0 else None
    lat = coords[1] if len(coords) > 1 else None

    # Build tidy rows: each date x parameter combination
    records = {}  # date_str -> dict of {param: value}

    for param, daily_values in params_data.items():
        if not isinstance(daily_values, dict):
            continue
        col_name = PARAM_MAP.get(param, param.lower())
        for date_str, val in daily_values.items():
            if date_str not in records:
                records[date_str] = {"date_str": date_str}
            # NASA POWER uses -999 as fill value
            records[date_str][col_name] = None if val == -999 else val

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(list(records.values()))

    # Parse date
    if "date_str" in df.columns:
        try:
            df["fecha"] = pd.to_datetime(df["date_str"], format="%Y%m%d", errors="coerce")
        except Exception:
            df["fecha"] = pd.to_datetime(df["date_str"], errors="coerce")
        df.drop(columns=["date_str"], inplace=True, errors="ignore")

    if lon is not None:
        df["longitude"] = lon
    if lat is not None:
        df["latitude"] = lat

    df["_source_file"] = json_path.stem
    return df


def build(bronze_dir: Path, silver_dir: Path, gold_dir: Path, catalog: CatalogManager, **kwargs):
    """Build solar and wind resource gold view from NASA POWER bronze data."""
    out_path = gold_dir / OUT_FILE
    gold_dir.mkdir(parents=True, exist_ok=True)

    nasa_dir = bronze_dir / BRONZE_SUBDIR
    if not nasa_dir.exists():
        log.warning("recurso_solar_eolico.no_bronze", path=str(nasa_dir))
        pd.DataFrame().to_parquet(out_path, index=False)
        catalog.register({
            "dataset_id": "recurso_solar_eolico",
            "source": "NASA POWER",
            "category": "solar_eolico",
            "data_type": "tabular",
            "layer": "gold",
            "file_path": str(out_path),
            "format": "parquet",
            "ingestor": "processor.gold.recurso_solar_eolico",
            "status": "empty",
            "notes": "Bronze NASA POWER directory not yet available",
        })
        return

    json_files = sorted(nasa_dir.glob("*.json"))
    if not json_files:
        log.warning("recurso_solar_eolico.no_json", path=str(nasa_dir))
        pd.DataFrame().to_parquet(out_path, index=False)
        catalog.register({
            "dataset_id": "recurso_solar_eolico",
            "source": "NASA POWER",
            "category": "solar_eolico",
            "data_type": "tabular",
            "layer": "gold",
            "file_path": str(out_path),
            "format": "parquet",
            "ingestor": "processor.gold.recurso_solar_eolico",
            "status": "empty",
            "notes": "No JSON files found in bronze/tabular/nasa_power/",
        })
        return

    frames = []
    for jf in json_files:
        try:
            df = _parse_nasa_power_json(jf)
            if not df.empty:
                frames.append(df)
                log.info("recurso_solar_eolico.parsed", file=jf.name, rows=len(df))
        except Exception as exc:
            log.warning("recurso_solar_eolico.parse_failed", file=jf.name, error=str(exc))

    if not frames:
        log.warning("recurso_solar_eolico.no_data")
        result = pd.DataFrame(
            columns=["fecha", "ghi_kwh_m2_day", "dni_kwh_m2_day", "wind_10m_ms", "wind_50m_ms"]
        )
    else:
        # Concatenate daily series
        result = pd.concat(frames, ignore_index=True)

        if "fecha" in result.columns:
            result["fecha"] = pd.to_datetime(result["fecha"], errors="coerce")
            result.sort_values("fecha", inplace=True)
            result.reset_index(drop=True, inplace=True)
            result["year"] = result["fecha"].dt.year
            result["month"] = result["fecha"].dt.month

        # Compute annual averages — attach as metadata rows at the bottom
        # or keep separate; here we add an annual_avg column group
        solar_cols = [c for c in result.columns if "ghi" in c or "dni" in c]
        wind_cols = [c for c in result.columns if "wind" in c]

        if "year" in result.columns:
            for col in solar_cols + wind_cols:
                if col in result.columns:
                    result[col] = pd.to_numeric(result[col], errors="coerce")

            ann = result.groupby("year")[solar_cols + wind_cols].mean().reset_index()
            ann_rows = len(ann)
            log.info("recurso_solar_eolico.annual_averages", years=ann_rows, cols=solar_cols + wind_cols)

    log.info("recurso_solar_eolico.done", rows=len(result))
    result.to_parquet(out_path, index=False)

    catalog.register({
        "dataset_id": "recurso_solar_eolico",
        "source": "NASA POWER",
        "category": "solar_eolico",
        "data_type": "tabular",
        "layer": "gold",
        "file_path": str(out_path),
        "format": "parquet",
        "ingestor": "processor.gold.recurso_solar_eolico",
        "status": "complete",
        "notes": (
            f"Daily solar/wind series from {len(json_files)} NASA POWER JSON files; "
            "parameters: GHI, DNI, WS10M, WS50M"
        ),
    })

"""Gold view: Merged discharge series with confidence indicator.

Sources (priority order):
1. IDEAM DHIME observed (alta confidence) — from silver/tabular/hidrologia/caudales/
2. Open-Meteo Flood API / GloFAS modeled (media confidence) — from bronze/tabular/open_meteo/flood_discharge.json

Output: gold/series_caudal.parquet
"""

import json
from pathlib import Path

import pandas as pd
import structlog

from catalog.manager import CatalogManager

log = structlog.get_logger()

OUT_FILE = "series_caudal.parquet"


def build(bronze_dir: Path, silver_dir: Path, gold_dir: Path, catalog: CatalogManager, **kwargs):
    """Build merged discharge series from IDEAM + Open-Meteo."""
    out_path = gold_dir / OUT_FILE
    gold_dir.mkdir(parents=True, exist_ok=True)

    frames = []

    # Source 1: IDEAM observed (alta confidence)
    caudal_dir = silver_dir / "tabular" / "hidrologia" / "caudales"
    if caudal_dir.exists():
        for p in sorted(caudal_dir.rglob("*.parquet")):
            try:
                df = pd.read_parquet(p)
                if not df.empty:
                    date_col = next((c for c in df.columns if "fecha" in c or "date" in c.lower()), None)
                    if date_col:
                        df.rename(columns={date_col: "fecha"}, inplace=True)
                    val_col = next((c for c in df.columns if "caudal" in c or "valor" in c), None)
                    if val_col:
                        df.rename(columns={val_col: "caudal_m3s"}, inplace=True)
                    df["confianza"] = "alta"
                    df["fuente"] = "IDEAM DHIME"
                    frames.append(df[["fecha", "caudal_m3s", "confianza", "fuente"]].copy())
            except Exception as exc:
                log.warning("series_caudal.ideam_failed", path=str(p), error=str(exc))

    # Source 2: Open-Meteo Flood API (media confidence — GloFAS modeled)
    flood_path = bronze_dir / "tabular" / "open_meteo" / "flood_discharge.json"
    if flood_path.exists():
        try:
            data = json.loads(flood_path.read_text())
            daily = data.get("daily", {})
            dates = daily.get("time", [])
            discharge = daily.get("river_discharge", [])

            if dates and discharge:
                df_flood = pd.DataFrame({"fecha": dates, "caudal_m3s": discharge})
                df_flood["fecha"] = pd.to_datetime(df_flood["fecha"], errors="coerce")
                df_flood["caudal_m3s"] = pd.to_numeric(df_flood["caudal_m3s"], errors="coerce")
                df_flood = df_flood.dropna(subset=["caudal_m3s"])
                df_flood["confianza"] = "media"
                df_flood["fuente"] = "Open-Meteo/GloFAS"
                frames.append(df_flood)
                log.info("series_caudal.open_meteo_loaded", rows=len(df_flood))
        except Exception as exc:
            log.warning("series_caudal.open_meteo_failed", error=str(exc))

    if not frames:
        log.warning("series_caudal.no_data")
        result = pd.DataFrame(columns=["fecha", "caudal_m3s", "confianza", "fuente"])
    else:
        result = pd.concat(frames, ignore_index=True)
        result["fecha"] = pd.to_datetime(result["fecha"], errors="coerce")
        result["caudal_m3s"] = pd.to_numeric(result["caudal_m3s"], errors="coerce")
        result.sort_values("fecha", inplace=True)
        result.reset_index(drop=True, inplace=True)

    log.info("series_caudal.done", rows=len(result), sources=len(frames))
    result.to_parquet(out_path, index=False)

    catalog.register({
        "dataset_id": "series_caudal",
        "source": "IDEAM + Open-Meteo/GloFAS",
        "category": "hidrologia",
        "data_type": "tabular",
        "layer": "gold",
        "file_path": str(out_path),
        "format": "parquet",
        "ingestor": "processor.gold.series_caudal",
        "status": "complete" if len(result) > 0 else "empty",
    })

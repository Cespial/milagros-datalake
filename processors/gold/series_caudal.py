"""Gold view: Merged discharge series with confidence indicator.

Reads IDEAM caudales from silver/tabular/hidrologia/caudales/,
merges all stations into a single series, and adds a confidence
indicator ("alta" for directly observed data). Output:
gold/series_caudal.parquet.
"""

from pathlib import Path

import pandas as pd
import structlog

from catalog.manager import CatalogManager

log = structlog.get_logger()

SILVER_SUBDIR = "tabular/hidrologia/caudales"
OUT_FILE = "series_caudal.parquet"


def build(bronze_dir: Path, silver_dir: Path, gold_dir: Path, catalog: CatalogManager, **kwargs):
    """Build merged discharge series gold view."""
    out_path = gold_dir / OUT_FILE
    gold_dir.mkdir(parents=True, exist_ok=True)

    caudal_dir = silver_dir / SILVER_SUBDIR
    if not caudal_dir.exists():
        log.warning("series_caudal.no_silver", path=str(caudal_dir))
        pd.DataFrame().to_parquet(out_path, index=False)
        catalog.register({
            "dataset_id": "series_caudal",
            "source": "IDEAM DHIME",
            "category": "hidrologia",
            "data_type": "tabular",
            "layer": "gold",
            "file_path": str(out_path),
            "format": "parquet",
            "ingestor": "processor.gold.series_caudal",
            "status": "empty",
            "notes": "Silver caudales data not yet available",
        })
        return

    frames = []
    for p in sorted(caudal_dir.rglob("*.parquet")):
        try:
            df = pd.read_parquet(p)
            if not df.empty:
                frames.append(df)
        except Exception as exc:
            log.warning("series_caudal.read_failed", path=str(p), error=str(exc))

    if not frames:
        log.warning("series_caudal.no_data")
        result = pd.DataFrame(columns=["fecha", "estacion", "caudal_m3s", "confianza"])
    else:
        result = pd.concat(frames, ignore_index=True)

        # Standardize date column
        date_col = next((c for c in result.columns if "fecha" in c or "date" in c.lower()), None)
        if date_col and date_col != "fecha":
            result.rename(columns={date_col: "fecha"}, inplace=True)

        # Standardize caudal value column
        val_col = next(
            (c for c in result.columns if "caudal" in c or "valor" in c or "q_" in c),
            None,
        )
        if val_col and val_col != "caudal_m3s":
            result.rename(columns={val_col: "caudal_m3s"}, inplace=True)

        if "fecha" in result.columns:
            result["fecha"] = pd.to_datetime(result["fecha"], errors="coerce")
            result.sort_values("fecha", inplace=True)
            result.reset_index(drop=True, inplace=True)

        # Add confidence indicator — observed IDEAM data = "alta"
        result["confianza"] = "alta"
        result["fuente"] = "IDEAM DHIME"

        # Coerce numeric
        if "caudal_m3s" in result.columns:
            result["caudal_m3s"] = pd.to_numeric(result["caudal_m3s"], errors="coerce")

    log.info("series_caudal.done", rows=len(result))
    result.to_parquet(out_path, index=False)

    catalog.register({
        "dataset_id": "series_caudal",
        "source": "IDEAM DHIME",
        "category": "hidrologia",
        "data_type": "tabular",
        "layer": "gold",
        "file_path": str(out_path),
        "format": "parquet",
        "ingestor": "processor.gold.series_caudal",
        "status": "complete",
        "notes": "Merged discharge series with confidence='alta' for observed data",
    })

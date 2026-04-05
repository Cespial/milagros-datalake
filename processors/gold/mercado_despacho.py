"""Gold view: Electricity market dispatch aggregation.

Reads XM SIMEM data from silver/tabular/mercado_electrico/ (PrecBolsa,
Gene, DemaSIN, AporEner, VoluUtil) and produces aggregated time series
suitable for feasibility analysis.
Output: gold/mercado_despacho.parquet.
"""

from pathlib import Path

import pandas as pd
import structlog

from catalog.manager import CatalogManager

log = structlog.get_logger()

SILVER_SUBDIR = "tabular/mercado_electrico"
OUT_FILE = "mercado_despacho.parquet"


def _read_parquet_dir(directory: Path) -> pd.DataFrame:
    """Read all parquet files recursively from a directory."""
    frames = []
    for p in sorted(directory.rglob("*.parquet")):
        try:
            df = pd.read_parquet(p)
            if not df.empty:
                df["_subdir"] = directory.name
                frames.append(df)
        except Exception as exc:
            log.warning("mercado_despacho.read_failed", path=str(p), error=str(exc))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def build(bronze_dir: Path, silver_dir: Path, gold_dir: Path, catalog: CatalogManager, **kwargs):
    """Build market dispatch gold view from silver XM SIMEM data."""
    out_path = gold_dir / OUT_FILE
    gold_dir.mkdir(parents=True, exist_ok=True)

    mercado_dir = silver_dir / SILVER_SUBDIR
    if not mercado_dir.exists():
        log.warning("mercado_despacho.no_silver", path=str(mercado_dir))
        pd.DataFrame().to_parquet(out_path, index=False)
        catalog.register({
            "dataset_id": "mercado_despacho",
            "source": "XM SIMEM",
            "category": "mercado_electrico",
            "data_type": "tabular",
            "layer": "gold",
            "file_path": str(out_path),
            "format": "parquet",
            "ingestor": "processor.gold.mercado_despacho",
            "status": "empty",
            "notes": "Silver mercado_electrico directory not yet available",
        })
        return

    # Discover sub-dataset directories
    source_dirs = sorted(d for d in mercado_dir.iterdir() if d.is_dir())
    if not source_dirs:
        log.warning("mercado_despacho.no_subdirs", path=str(mercado_dir))

    all_frames = []

    for src_dir in source_dirs:
        df = _read_parquet_dir(src_dir)
        if df.empty:
            continue
        df["dataset"] = src_dir.name
        all_frames.append(df)
        log.info("mercado_despacho.loaded", source=src_dir.name, rows=len(df))

    if not all_frames:
        log.warning("mercado_despacho.no_data")
        result = pd.DataFrame(columns=["dataset", "fecha", "valor"])
    else:
        result = pd.concat(all_frames, ignore_index=True)

        # Detect and standardize date column
        date_col = next(
            (c for c in result.columns if c in ("fecha", "date") or "fecha" in c),
            None,
        )
        if date_col:
            result["fecha"] = pd.to_datetime(result[date_col], errors="coerce")
            result["year"] = result["fecha"].dt.year
            result["month"] = result["fecha"].dt.month

        # Detect numeric value column
        numeric_cols = result.select_dtypes(include="number").columns.tolist()
        meta_cols = {"year", "month", "_subdir"}
        value_cols = [c for c in numeric_cols if c not in meta_cols]

        log.info("mercado_despacho.columns", value_cols=value_cols)

    log.info("mercado_despacho.done", rows=len(result))
    result.to_parquet(out_path, index=False)

    catalog.register({
        "dataset_id": "mercado_despacho",
        "source": "XM SIMEM",
        "category": "mercado_electrico",
        "data_type": "tabular",
        "layer": "gold",
        "file_path": str(out_path),
        "format": "parquet",
        "ingestor": "processor.gold.mercado_despacho",
        "status": "complete",
        "notes": (
            f"Aggregated XM market data from {len(source_dirs)} sub-datasets "
            "(PrecBolsa, Gene, DemaSIN, AporEner, VoluUtil)"
        ),
    })

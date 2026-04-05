"""Gold view: Natural hazards summary.

Combines SGC sismicidad + SIMMA mass movements + DesInventar disaster records
from silver/tabular/amenazas/. Computes summary statistics and sub-tables.
Output: gold/amenazas_naturales.parquet + sub-tables
  gold/amenazas_sismicidad.parquet
  gold/amenazas_movimientos.parquet
  gold/amenazas_historicos.parquet
"""

from pathlib import Path

import pandas as pd
import structlog

from catalog.manager import CatalogManager

log = structlog.get_logger()

SILVER_SUBDIR = "tabular/amenazas"
OUT_FILE = "amenazas_naturales.parquet"

SOURCES = [
    {
        "subdir": "sgc_sismicidad",
        "dataset_id": "amenazas_sismicidad",
        "out_file": "amenazas_sismicidad.parquet",
        "category": "geologia",
        "source_name": "SGC Sismicidad",
    },
    {
        "subdir": "sgc_simma",
        "dataset_id": "amenazas_movimientos",
        "out_file": "amenazas_movimientos.parquet",
        "category": "geologia",
        "source_name": "SGC SIMMA",
    },
    {
        "subdir": "desinventar",
        "dataset_id": "amenazas_historicos",
        "out_file": "amenazas_historicos.parquet",
        "category": "geologia",
        "source_name": "DesInventar",
    },
]


def _read_parquet_dir(directory: Path) -> pd.DataFrame:
    """Read all parquet files recursively from a directory."""
    frames = []
    for p in sorted(directory.rglob("*.parquet")):
        try:
            df = pd.read_parquet(p)
            if not df.empty:
                frames.append(df)
        except Exception as exc:
            log.warning("amenazas_naturales.read_failed", path=str(p), error=str(exc))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def build(bronze_dir: Path, silver_dir: Path, gold_dir: Path, catalog: CatalogManager, **kwargs):
    """Build natural hazards summary from silver amenazas data."""
    gold_dir.mkdir(parents=True, exist_ok=True)
    out_path = gold_dir / OUT_FILE

    amenazas_dir = silver_dir / SILVER_SUBDIR
    if not amenazas_dir.exists():
        log.warning("amenazas_naturales.no_silver", path=str(amenazas_dir))
        pd.DataFrame().to_parquet(out_path, index=False)
        catalog.register({
            "dataset_id": "amenazas_naturales",
            "source": "SGC / DesInventar",
            "category": "geologia",
            "data_type": "tabular",
            "layer": "gold",
            "file_path": str(out_path),
            "format": "parquet",
            "ingestor": "processor.gold.amenazas_naturales",
            "status": "empty",
            "notes": "Silver amenazas directory not yet available",
        })
        return

    summary_rows = []

    for spec in SOURCES:
        source_dir = amenazas_dir / spec["subdir"]
        sub_out_path = gold_dir / spec["out_file"]

        if not source_dir.exists():
            log.warning("amenazas_naturales.missing_source", source=spec["subdir"])
            pd.DataFrame().to_parquet(sub_out_path, index=False)
            catalog.register({
                "dataset_id": spec["dataset_id"],
                "source": spec["source_name"],
                "category": spec["category"],
                "data_type": "tabular",
                "layer": "gold",
                "file_path": str(sub_out_path),
                "format": "parquet",
                "ingestor": "processor.gold.amenazas_naturales",
                "status": "empty",
                "notes": f"Source directory not found: {spec['subdir']}",
            })
            continue

        df = _read_parquet_dir(source_dir)

        if df.empty:
            log.warning("amenazas_naturales.empty_source", source=spec["subdir"])
            pd.DataFrame().to_parquet(sub_out_path, index=False)
            catalog.register({
                "dataset_id": spec["dataset_id"],
                "source": spec["source_name"],
                "category": spec["category"],
                "data_type": "tabular",
                "layer": "gold",
                "file_path": str(sub_out_path),
                "format": "parquet",
                "ingestor": "processor.gold.amenazas_naturales",
                "status": "empty",
                "notes": "No data in silver layer",
            })
            summary_rows.append({
                "fuente": spec["source_name"],
                "registros": 0,
                "columnas": 0,
            })
            continue

        df.to_parquet(sub_out_path, index=False)

        catalog.register({
            "dataset_id": spec["dataset_id"],
            "source": spec["source_name"],
            "category": spec["category"],
            "data_type": "tabular",
            "layer": "gold",
            "file_path": str(sub_out_path),
            "format": "parquet",
            "ingestor": "processor.gold.amenazas_naturales",
            "status": "complete",
            "notes": f"{len(df)} records, {len(df.columns)} columns from {spec['subdir']}",
        })

        summary_rows.append({
            "fuente": spec["source_name"],
            "registros": len(df),
            "columnas": len(df.columns),
        })
        log.info("amenazas_naturales.source_done", source=spec["subdir"], rows=len(df))

    # Build summary table
    summary = pd.DataFrame(summary_rows) if summary_rows else pd.DataFrame(
        columns=["fuente", "registros", "columnas"]
    )
    summary.to_parquet(out_path, index=False)
    log.info("amenazas_naturales.done", sources=len(summary_rows))

    catalog.register({
        "dataset_id": "amenazas_naturales",
        "source": "SGC / DesInventar",
        "category": "geologia",
        "data_type": "tabular",
        "layer": "gold",
        "file_path": str(out_path),
        "format": "parquet",
        "ingestor": "processor.gold.amenazas_naturales",
        "status": "complete",
        "notes": "Summary of SGC sismicidad, SIMMA mass movements, and DesInventar disasters",
    })

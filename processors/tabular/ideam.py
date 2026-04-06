"""Silver processor: IDEAM nivel de rio + precipitacion → standardized Parquet.

Reads from bronze/tabular/ideam_dhime/ (nivel_instantaneo, nivel_maximo, nivel_minimo, precipitacion).
Writes to silver/tabular/hidrologia/ideam_{variable}/ partitioned by year.
"""

import json
from pathlib import Path

import pandas as pd
import structlog

from processors.base import TabularProcessor

log = structlog.get_logger()

VARIABLES = {
    "nivel_instantaneo": {"unit": "cm", "description": "Nivel instantaneo del rio"},
    "nivel_maximo": {"unit": "cm", "description": "Nivel maximo diario del rio"},
    "nivel_minimo": {"unit": "cm", "description": "Nivel minimo diario del rio"},
    "precipitacion": {"unit": "mm", "description": "Precipitacion"},
}


def process(bronze_dir: Path, silver_dir: Path, catalog, **kwargs):
    """Process IDEAM hydrology JSON files to Silver Parquet."""
    ideam_dir = bronze_dir / "tabular" / "ideam_dhime"
    out_base = silver_dir / "tabular" / "hidrologia"

    if not ideam_dir.exists():
        log.warning("ideam.no_bronze")
        return

    for var_name, meta in VARIABLES.items():
        json_file = ideam_dir / f"{var_name}.json"
        if not json_file.exists() or json_file.stat().st_size < 100:
            log.warning("ideam.skip_empty", variable=var_name)
            continue

        log.info("ideam.processing", variable=var_name, file=str(json_file))

        # Read JSON in chunks to handle large files
        with open(json_file) as f:
            records = json.load(f)

        if not records:
            log.warning("ideam.no_records", variable=var_name)
            continue

        df = pd.DataFrame(records)
        log.info("ideam.loaded", variable=var_name, rows=len(df))

        # Standardize columns
        df = TabularProcessor.standardize_columns(df)
        df = TabularProcessor.clean_nulls(df)

        # Parse date
        date_col = None
        for col in df.columns:
            if "fecha" in col:
                date_col = col
                break

        if date_col:
            df["fecha"] = pd.to_datetime(df[date_col], errors="coerce")

        # Parse numeric value
        val_col = None
        for col in df.columns:
            if "valor" in col:
                val_col = col
                break

        if val_col:
            df[val_col] = pd.to_numeric(df[val_col], errors="coerce")

        # Write partitioned
        dest_dir = out_base / f"ideam_{var_name}"
        dest_dir.mkdir(parents=True, exist_ok=True)
        TabularProcessor.write_partitioned(df, dest_dir)

        parquet_files = list(dest_dir.rglob("*.parquet"))
        log.info("ideam.done", variable=var_name, partitions=len(parquet_files))

        for p in parquet_files:
            catalog.register({
                "dataset_id": f"ideam_{var_name}",
                "source": "IDEAM DHIME",
                "category": "hidrologia",
                "data_type": "tabular",
                "layer": "silver",
                "file_path": str(p),
                "format": "parquet",
                "ingestor": "processors.tabular.ideam",
                "status": "complete",
                "notes": f"{meta['description']} ({meta['unit']})",
            })

"""Silver processor: XM SIMEM CSVs → standardized Parquet.

Reads bronze/tabular/xm_simem/*.csv (PrecBolsa, Gene, DemaSIN, AporEner, VoluUtil),
cleans nulls, standardizes column names, and writes Parquet to
silver/tabular/mercado_electrico/.
"""

from pathlib import Path

import structlog

from processors.base import TabularProcessor

log = structlog.get_logger()

SOURCE_SUBDIR = "tabular/xm_simem"
OUT_SUBDIR = "tabular/mercado_electrico"


def process(bronze_dir: Path, silver_dir: Path, catalog, **kwargs) -> list[Path]:
    """Process XM SIMEM CSV files to Silver Parquet."""
    import pandas as pd

    bronze_path = bronze_dir / SOURCE_SUBDIR
    out_dir = silver_dir / OUT_SUBDIR
    out_dir.mkdir(parents=True, exist_ok=True)

    csv_files = sorted(bronze_path.glob("*.csv")) if bronze_path.exists() else []
    if not csv_files:
        log.warning("mercado_electrico.no_bronze_files", path=str(bronze_path))
        return []

    written: list[Path] = []

    for csv_file in csv_files:
        dataset = csv_file.stem  # e.g. "PrecBolsa", "Gene"
        log.info("mercado_electrico.read", file=str(csv_file), dataset=dataset)

        try:
            df = pd.read_csv(str(csv_file))
        except Exception as exc:
            log.error("mercado_electrico.read_failed", file=str(csv_file), error=str(exc))
            continue

        if df.empty:
            log.warning("mercado_electrico.empty", file=str(csv_file))
            continue

        df = TabularProcessor.standardize_columns(df)
        df = TabularProcessor.clean_nulls(df)

        dest_dir = out_dir / dataset.lower()
        paths = TabularProcessor.write_partitioned(df, dest_dir)
        written.extend(paths)

        for p in paths:
            catalog.register({
                "dataset_id": f"xm_simem_{dataset.lower()}",
                "source": "XM SIMEM",
                "category": "mercado_electrico",
                "data_type": "tabular",
                "layer": "silver",
                "file_path": str(p),
                "format": "parquet",
                "license": "XM Open Data",
                "ingestor": "processors.tabular.mercado_electrico",
                "status": "complete",
                "notes": f"Processed from bronze CSV: {csv_file.name}",
            })

    log.info("mercado_electrico.done", files_written=len(written))
    return written

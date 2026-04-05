"""Silver processor: IDEAM DHIME JSON → standardized Parquet.

Reads bronze/tabular/ideam_dhime/*.json (caudales, precipitacion),
cleans nulls, standardizes column names, and writes partitioned Parquet
to silver/tabular/hidrologia/.
"""

from pathlib import Path

import structlog

from processors.base import TabularProcessor

log = structlog.get_logger()

DATASET_ID = "ideam_dhime"
SOURCE_SUBDIR = "tabular/ideam_dhime"
OUT_SUBDIR = "tabular/hidrologia"


def process(bronze_dir: Path, silver_dir: Path, catalog, **kwargs) -> list[Path]:
    """Process IDEAM DHIME hydrology JSON files to Silver Parquet."""
    import json
    import pandas as pd

    bronze_path = bronze_dir / SOURCE_SUBDIR
    out_dir = silver_dir / OUT_SUBDIR
    out_dir.mkdir(parents=True, exist_ok=True)

    json_files = sorted(bronze_path.glob("*.json")) if bronze_path.exists() else []
    if not json_files:
        log.warning("hidrologia.no_bronze_files", path=str(bronze_path))
        return []

    written: list[Path] = []

    for json_file in json_files:
        variable = json_file.stem  # e.g. "caudales", "precipitacion"
        log.info("hidrologia.read", file=str(json_file), variable=variable)

        with open(json_file, encoding="utf-8") as fh:
            records = json.load(fh)

        if not records:
            log.warning("hidrologia.empty", file=str(json_file))
            continue

        df = pd.DataFrame(records)
        df = TabularProcessor.standardize_columns(df)
        df = TabularProcessor.clean_nulls(df)

        dest_dir = out_dir / variable
        paths = TabularProcessor.write_partitioned(df, dest_dir)
        written.extend(paths)

        for p in paths:
            catalog.register({
                "dataset_id": f"ideam_dhime_{variable}",
                "source": "IDEAM DHIME",
                "category": "hidrologia",
                "data_type": "tabular",
                "layer": "silver",
                "file_path": str(p),
                "format": "parquet",
                "license": "CC0",
                "ingestor": "processors.tabular.hidrologia",
                "status": "complete",
                "notes": f"Processed from bronze JSON: {json_file.name}",
            })

    log.info("hidrologia.done", files_written=len(written))
    return written

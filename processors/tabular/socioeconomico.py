"""Silver processor: DANE + DNP TerriData + AGRONET → standardized Parquet.

Sources:
  - bronze/tabular/dane_censo/       — DANE population census data
  - bronze/tabular/dnp_terridata/    — DNP TerriData socioeconomic indicators
  - bronze/tabular/agronet_eva/      — AGRONET agricultural evaluation data

Output: silver/tabular/socioeconomico/<source>/data.parquet
"""

from pathlib import Path

import structlog

from processors.base import TabularProcessor

log = structlog.get_logger()

SOURCES = [
    {"subdir": "tabular/dane_censo",    "dataset_id": "dane_censo",    "source_name": "DANE Censo",     "license": "CC0"},
    {"subdir": "tabular/dnp_terridata", "dataset_id": "dnp_terridata", "source_name": "DNP TerriData",  "license": "CC0"},
    {"subdir": "tabular/agronet_eva",   "dataset_id": "agronet_eva",   "source_name": "AGRONET EVA",    "license": "CC0"},
]

OUT_SUBDIR = "tabular/socioeconomico"


def _read_tabular_files(bronze_path: Path):
    """Read CSVs and JSONs from a directory, return concatenated DataFrame or None."""
    import pandas as pd

    frames = []

    for csv_file in sorted(bronze_path.glob("*.csv")):
        try:
            df = pd.read_csv(str(csv_file))
            if not df.empty:
                df["_source_file"] = csv_file.name
                frames.append(df)
        except Exception as exc:
            log.error("socioeconomico.read_csv_failed", file=str(csv_file), error=str(exc))

    for json_file in sorted(bronze_path.glob("*.json")):
        try:
            df = pd.read_json(str(json_file))
            if not df.empty:
                df["_source_file"] = json_file.name
                frames.append(df)
        except Exception as exc:
            log.error("socioeconomico.read_json_failed", file=str(json_file), error=str(exc))

    if not frames:
        return None
    return pd.concat(frames, ignore_index=True)


def process(bronze_dir: Path, silver_dir: Path, catalog, **kwargs) -> list[Path]:
    """Process DANE/DNP/AGRONET files to Silver Parquet."""
    out_dir = silver_dir / OUT_SUBDIR
    out_dir.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []

    for spec in SOURCES:
        bronze_path = bronze_dir / spec["subdir"]
        if not bronze_path.exists():
            log.warning("socioeconomico.missing_bronze", path=str(bronze_path))
            continue

        log.info("socioeconomico.processing", source=spec["dataset_id"])
        df = _read_tabular_files(bronze_path)

        if df is None or df.empty:
            log.warning("socioeconomico.empty", source=spec["dataset_id"])
            continue

        df = TabularProcessor.standardize_columns(df)
        df = TabularProcessor.clean_nulls(df)

        dest_dir = out_dir / spec["dataset_id"]
        TabularProcessor.write_partitioned(df, dest_dir)
        parquet_files = list(dest_dir.rglob("*.parquet"))
        written.extend(parquet_files)

        for p in parquet_files:
            catalog.register({
                "dataset_id": spec["dataset_id"],
                "source": spec["source_name"],
                "category": "socioeconomico",
                "data_type": "tabular",
                "layer": "silver",
                "file_path": str(p),
                "format": "parquet",
                "license": spec["license"],
                "ingestor": "processors.tabular.socioeconomico",
                "status": "complete",
                "notes": f"Merged from {bronze_path.name}",
            })

    log.info("socioeconomico.done", files_written=len(written))
    return written

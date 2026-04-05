"""Silver processor: SGC sismicidad + SIMMA + DesInventar → standardized Parquet.

Sources:
  - bronze/tabular/sgc_sismicidad/   — SGC seismicity (JSON with GeoJSON features)
  - bronze/tabular/sgc_simma/        — SIMMA mass-movement inventory
  - bronze/tabular/desinventar/      — DesInventar disaster records

Parses USGS/SGC GeoJSON features into tabular rows when applicable.
Output: silver/tabular/amenazas/<source>/data.parquet
"""

from pathlib import Path

import structlog

from processors.base import TabularProcessor

log = structlog.get_logger()

SOURCES = [
    {
        "subdir": "tabular/sgc_sismicidad",
        "dataset_id": "sgc_sismicidad",
        "source_name": "SGC Sismicidad",
        "license": "CC0",
        "geojson": True,
    },
    {
        "subdir": "tabular/sgc_simma",
        "dataset_id": "sgc_simma",
        "source_name": "SGC SIMMA",
        "license": "CC0",
        "geojson": False,
    },
    {
        "subdir": "tabular/desinventar",
        "dataset_id": "desinventar",
        "source_name": "DesInventar",
        "license": "CC0",
        "geojson": False,
    },
]

OUT_SUBDIR = "tabular/amenazas"


def _parse_geojson_features(path: Path):
    """Parse a GeoJSON FeatureCollection JSON into a flat DataFrame."""
    import json
    import pandas as pd

    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)

    # Handle both raw list of records and GeoJSON FeatureCollection
    if isinstance(data, list):
        return pd.DataFrame(data)

    features = data.get("features", [])
    if not features:
        return pd.DataFrame()

    rows = []
    for feat in features:
        row = dict(feat.get("properties") or {})
        geom = feat.get("geometry")
        if geom and geom.get("type") == "Point":
            coords = geom.get("coordinates", [])
            if len(coords) >= 2:
                row["longitude"] = coords[0]
                row["latitude"] = coords[1]
            if len(coords) >= 3:
                row["depth_km"] = coords[2]
        rows.append(row)

    return pd.DataFrame(rows)


def _read_source(bronze_path: Path, geojson: bool):
    """Read all files in bronze_path, return concatenated DataFrame or None."""
    import pandas as pd

    frames = []

    for json_file in sorted(bronze_path.glob("*.json")):
        try:
            if geojson:
                df = _parse_geojson_features(json_file)
            else:
                df = pd.read_json(str(json_file))
            if not df.empty:
                df["_source_file"] = json_file.name
                frames.append(df)
        except Exception as exc:
            log.error("amenazas.read_json_failed", file=str(json_file), error=str(exc))

    for csv_file in sorted(bronze_path.glob("*.csv")):
        try:
            df = pd.read_csv(str(csv_file))
            if not df.empty:
                df["_source_file"] = csv_file.name
                frames.append(df)
        except Exception as exc:
            log.error("amenazas.read_csv_failed", file=str(csv_file), error=str(exc))

    if not frames:
        return None
    return pd.concat(frames, ignore_index=True)


def process(bronze_dir: Path, silver_dir: Path, catalog, **kwargs) -> list[Path]:
    """Process SGC/SIMMA/DesInventar hazard files to Silver Parquet."""
    out_dir = silver_dir / OUT_SUBDIR
    out_dir.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []

    for spec in SOURCES:
        bronze_path = bronze_dir / spec["subdir"]
        if not bronze_path.exists():
            log.warning("amenazas.missing_bronze", path=str(bronze_path))
            continue

        log.info("amenazas.processing", source=spec["dataset_id"])
        df = _read_source(bronze_path, geojson=spec["geojson"])

        if df is None or df.empty:
            log.warning("amenazas.empty", source=spec["dataset_id"])
            continue

        df = TabularProcessor.standardize_columns(df)
        df = TabularProcessor.clean_nulls(df)

        dest_dir = out_dir / spec["dataset_id"]
        paths = TabularProcessor.write_partitioned(df, dest_dir)
        written.extend(paths)

        for p in paths:
            catalog.register({
                "dataset_id": spec["dataset_id"],
                "source": spec["source_name"],
                "category": "geologia",
                "data_type": "tabular",
                "layer": "silver",
                "file_path": str(p),
                "format": "parquet",
                "license": spec["license"],
                "ingestor": "processors.tabular.amenazas",
                "status": "complete",
                "notes": f"Processed from {bronze_path.name}",
            })

    log.info("amenazas.done", files_written=len(written))
    return written

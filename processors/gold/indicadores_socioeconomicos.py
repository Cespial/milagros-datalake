"""Gold view: Municipal socioeconomic profiles.

Reads DANE censo + DNP TerriData + AGRONET EVA from silver/tabular/socioeconomico/,
filters to AOI_MUNICIPIOS, and produces a unified municipal profile table.
Output: gold/indicadores_socioeconomicos.parquet.
"""

from pathlib import Path

import pandas as pd
import structlog

from catalog.manager import CatalogManager
from config.settings import AOI_MUNICIPIOS

log = structlog.get_logger()

SILVER_SUBDIR = "tabular/socioeconomico"
OUT_FILE = "indicadores_socioeconomicos.parquet"

SOURCES = [
    {"subdir": "dane_censo",    "dataset_id": "dane_censo",    "source_name": "DANE Censo"},
    {"subdir": "dnp_terridata", "dataset_id": "dnp_terridata", "source_name": "DNP TerriData"},
    {"subdir": "agronet_eva",   "dataset_id": "agronet_eva",   "source_name": "AGRONET EVA"},
]

# DANE municipality code column candidates
MUNICIPIO_COLS = ["cod_mpio", "codigo_municipio", "municipio_code", "divipola", "cod_municipio"]


def _read_parquet_dir(directory: Path) -> pd.DataFrame:
    """Read all parquet files recursively from a directory."""
    frames = []
    for p in sorted(directory.rglob("*.parquet")):
        try:
            df = pd.read_parquet(p)
            if not df.empty:
                frames.append(df)
        except Exception as exc:
            log.warning("indicadores_socioeconomicos.read_failed", path=str(p), error=str(exc))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _filter_aoi(df: pd.DataFrame) -> pd.DataFrame:
    """Filter rows matching AOI municipios if a code column is found."""
    for col in MUNICIPIO_COLS:
        if col in df.columns:
            df[col] = df[col].astype(str).str.zfill(5)
            aoi_codes = set(AOI_MUNICIPIOS.keys())
            filtered = df[df[col].isin(aoi_codes)]
            if not filtered.empty:
                log.info("indicadores_socioeconomicos.aoi_filter", col=col, rows=len(filtered))
                return filtered
    # If no municipal code column found, return all data with a warning
    log.warning("indicadores_socioeconomicos.no_municipio_col", columns=list(df.columns)[:10])
    return df


def build(bronze_dir: Path, silver_dir: Path, gold_dir: Path, catalog: CatalogManager, **kwargs):
    """Build municipal socioeconomic profiles gold view."""
    out_path = gold_dir / OUT_FILE
    gold_dir.mkdir(parents=True, exist_ok=True)

    socio_dir = silver_dir / SILVER_SUBDIR
    if not socio_dir.exists():
        log.warning("indicadores_socioeconomicos.no_silver", path=str(socio_dir))
        pd.DataFrame().to_parquet(out_path, index=False)
        catalog.register({
            "dataset_id": "indicadores_socioeconomicos",
            "source": "DANE / DNP / AGRONET",
            "category": "socioeconomico",
            "data_type": "tabular",
            "layer": "gold",
            "file_path": str(out_path),
            "format": "parquet",
            "ingestor": "processor.gold.indicadores_socioeconomicos",
            "status": "empty",
            "notes": "Silver socioeconomico directory not yet available",
        })
        return

    all_frames = []

    for spec in SOURCES:
        src_dir = socio_dir / spec["subdir"]
        if not src_dir.exists():
            log.warning("indicadores_socioeconomicos.missing", source=spec["subdir"])
            continue

        df = _read_parquet_dir(src_dir)
        if df.empty:
            log.warning("indicadores_socioeconomicos.empty", source=spec["subdir"])
            continue

        df = _filter_aoi(df)
        df["_fuente"] = spec["source_name"]
        all_frames.append(df)
        log.info("indicadores_socioeconomicos.loaded", source=spec["subdir"], rows=len(df))

    if not all_frames:
        log.warning("indicadores_socioeconomicos.no_data")
        result = pd.DataFrame(columns=["_fuente"])
    else:
        result = pd.concat(all_frames, ignore_index=True)
        # Add human-readable municipality names where code is found
        for col in MUNICIPIO_COLS:
            if col in result.columns:
                result[col] = result[col].astype(str).str.zfill(5)
                result["nombre_municipio"] = result[col].map(AOI_MUNICIPIOS).fillna("")
                break

    log.info("indicadores_socioeconomicos.done", rows=len(result))
    result.to_parquet(out_path, index=False)

    catalog.register({
        "dataset_id": "indicadores_socioeconomicos",
        "source": "DANE / DNP / AGRONET",
        "category": "socioeconomico",
        "data_type": "tabular",
        "layer": "gold",
        "file_path": str(out_path),
        "format": "parquet",
        "ingestor": "processor.gold.indicadores_socioeconomicos",
        "status": "complete",
        "notes": (
            f"Municipal profiles for {len(AOI_MUNICIPIOS)} AOI municipalities "
            f"({', '.join(AOI_MUNICIPIOS.values())})"
        ),
    })

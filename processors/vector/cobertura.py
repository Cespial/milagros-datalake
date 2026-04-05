"""Silver processor: Corine LC + RUNAP + CORANTIOQUIA → clipped GeoParquet.

Reads:
  - bronze/vector/corine_lc/       — Corine Land Cover Colombia
  - bronze/vector/runap/           — RUNAP protected areas
  - bronze/vector/corantioquia/    — CORANTIOQUIA environmental management zones

Clips to study AOI, fixes geometries, and writes individual GeoParquet files
to silver/vector/cobertura/.
"""

from pathlib import Path

import structlog

from config.settings import AOI_BOUNDS
from processors.base import VectorProcessor

log = structlog.get_logger()

SOURCES = [
    {
        "subdir": "vector/corine_lc",
        "dataset_id": "corine_lc",
        "source_name": "Corine Land Cover Colombia",
        "license": "CC0",
        "category": "biodiversidad",
    },
    {
        "subdir": "vector/runap",
        "dataset_id": "runap",
        "source_name": "RUNAP Protected Areas",
        "license": "CC0",
        "category": "biodiversidad",
    },
    {
        "subdir": "vector/corantioquia",
        "dataset_id": "corantioquia",
        "source_name": "CORANTIOQUIA",
        "license": "CC0",
        "category": "regulatorio",
    },
]

OUT_SUBDIR = "vector/cobertura"
VECTOR_EXTENSIONS = ("*.shp", "*.gpkg", "*.geojson", "*.json")


def process(bronze_dir: Path, silver_dir: Path, catalog, **kwargs) -> list[Path]:
    """Process Corine/RUNAP/CORANTIOQUIA vector files to clipped GeoParquet."""
    import geopandas as gpd
    import pandas as pd

    out_dir = silver_dir / OUT_SUBDIR
    out_dir.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []

    for spec in SOURCES:
        bronze_path = bronze_dir / spec["subdir"]
        if not bronze_path.exists():
            log.warning("cobertura.missing_bronze", path=str(bronze_path))
            continue

        vector_files: list[Path] = []
        for ext in VECTOR_EXTENSIONS:
            vector_files.extend(sorted(bronze_path.glob(ext)))

        if not vector_files:
            log.warning("cobertura.no_files", path=str(bronze_path))
            continue

        frames: list[gpd.GeoDataFrame] = []
        for vf in vector_files:
            log.info("cobertura.read", file=str(vf))
            try:
                gdf = gpd.read_file(str(vf))
                if not gdf.empty:
                    frames.append(gdf)
            except Exception as exc:
                log.error("cobertura.read_failed", file=str(vf), error=str(exc))

        if not frames:
            continue

        combined = pd.concat(frames, ignore_index=True)
        combined = gpd.GeoDataFrame(combined, crs=frames[0].crs)
        combined = VectorProcessor.fix_geometries(combined)
        combined = VectorProcessor.clip_to_aoi(combined, AOI_BOUNDS)

        if combined.empty:
            log.warning("cobertura.empty_after_clip", source=spec["dataset_id"])
            continue

        out_path = out_dir / f"{spec['dataset_id']}.parquet"
        result = VectorProcessor.to_geoparquet(combined, out_path)
        written.append(result)

        catalog.register({
            "dataset_id": spec["dataset_id"],
            "source": spec["source_name"],
            "category": spec["category"],
            "data_type": "vector",
            "layer": "silver",
            "file_path": str(result),
            "format": "parquet",
            "crs": "EPSG:4326",
            "license": spec["license"],
            "ingestor": "processors.vector.cobertura",
            "status": "complete",
            "notes": f"Merged {len(vector_files)} layers, clipped to AOI",
        })
        log.info("cobertura.source_done", source=spec["dataset_id"], rows=len(combined))

    log.info("cobertura.done", files_written=len(written))
    return written

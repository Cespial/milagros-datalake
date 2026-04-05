"""Silver processor: SGC geology + IGAC cartography → clipped GeoParquet.

Reads:
  - bronze/vector/sgc_geologia/   — SGC geological map vector files
  - bronze/vector/igac_cartografia/ — IGAC base cartography layers

Clips to study AOI, fixes geometries, merges, and writes to
silver/vector/geologia/geologia.parquet.
"""

from pathlib import Path

import structlog

from config.settings import AOI_BOUNDS
from processors.base import VectorProcessor

log = structlog.get_logger()

SOURCES = [
    {"subdir": "vector/sgc_geologia",    "source_name": "SGC Geologia",     "dataset_id": "sgc_geologia"},
    {"subdir": "vector/igac_cartografia", "source_name": "IGAC Cartografia", "dataset_id": "igac_cartografia"},
]

OUT_SUBDIR = "vector/geologia"
VECTOR_EXTENSIONS = ("*.shp", "*.gpkg", "*.geojson", "*.json")


def process(bronze_dir: Path, silver_dir: Path, catalog, **kwargs) -> list[Path]:
    """Process SGC/IGAC geology vector files to clipped GeoParquet."""
    import geopandas as gpd
    import pandas as pd

    out_dir = silver_dir / OUT_SUBDIR
    out_dir.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []

    for spec in SOURCES:
        bronze_path = bronze_dir / spec["subdir"]
        if not bronze_path.exists():
            log.warning("geologia.missing_bronze", path=str(bronze_path))
            continue

        vector_files: list[Path] = []
        for ext in VECTOR_EXTENSIONS:
            vector_files.extend(sorted(bronze_path.glob(ext)))

        if not vector_files:
            log.warning("geologia.no_files", path=str(bronze_path))
            continue

        frames: list[gpd.GeoDataFrame] = []
        for vf in vector_files:
            log.info("geologia.read", file=str(vf))
            try:
                gdf = gpd.read_file(str(vf))
                if not gdf.empty:
                    frames.append(gdf)
            except Exception as exc:
                log.error("geologia.read_failed", file=str(vf), error=str(exc))

        if not frames:
            continue

        combined = pd.concat(frames, ignore_index=True)
        combined = gpd.GeoDataFrame(combined, crs=frames[0].crs)
        combined = VectorProcessor.fix_geometries(combined)
        combined = VectorProcessor.clip_to_aoi(combined, AOI_BOUNDS)

        if combined.empty:
            log.warning("geologia.empty_after_clip", source=spec["dataset_id"])
            continue

        out_path = out_dir / f"{spec['dataset_id']}.parquet"
        result = VectorProcessor.to_geoparquet(combined, out_path)
        written.append(result)

        catalog.register({
            "dataset_id": spec["dataset_id"],
            "source": spec["source_name"],
            "category": "geologia",
            "data_type": "vector",
            "layer": "silver",
            "file_path": str(result),
            "format": "parquet",
            "crs": "EPSG:4326",
            "license": "CC0",
            "ingestor": "processors.vector.geologia",
            "status": "complete",
            "notes": f"Merged {len(vector_files)} layers, clipped to AOI",
        })
        log.info("geologia.source_done", source=spec["dataset_id"], rows=len(combined))

    log.info("geologia.done", files_written=len(written))
    return written

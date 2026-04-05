"""Silver processor: HydroSHEDS watershed shapefiles → clipped GeoParquet.

Reads bronze/vector/hydrosheds/*.shp (or .gpkg), clips to study AOI,
fixes geometries, and writes to silver/vector/cuencas/cuencas.parquet.
"""

from pathlib import Path

import pandas as pd
import structlog

from config.settings import AOI_BOUNDS
from processors.base import VectorProcessor

log = structlog.get_logger()

SOURCE_SUBDIR = "vector/hydrosheds"
OUT_SUBDIR = "vector/cuencas"

VECTOR_EXTENSIONS = ("*.shp", "*.gpkg", "*.geojson", "*.json")


def process(bronze_dir: Path, silver_dir: Path, catalog, **kwargs) -> list[Path]:
    """Process HydroSHEDS shapefiles to clipped GeoParquet."""
    import geopandas as gpd

    bronze_path = bronze_dir / SOURCE_SUBDIR
    out_dir = silver_dir / OUT_SUBDIR
    out_dir.mkdir(parents=True, exist_ok=True)

    vector_files: list[Path] = []
    if bronze_path.exists():
        for ext in VECTOR_EXTENSIONS:
            vector_files.extend(sorted(bronze_path.glob(ext)))

    if not vector_files:
        log.warning("cuencas.no_bronze_files", path=str(bronze_path))
        return []

    frames: list[gpd.GeoDataFrame] = []

    for vf in vector_files:
        log.info("cuencas.read", file=str(vf))
        try:
            gdf = gpd.read_file(str(vf))
            if gdf.empty:
                continue
            frames.append(gdf)
        except Exception as exc:
            log.error("cuencas.read_failed", file=str(vf), error=str(exc))

    if not frames:
        log.warning("cuencas.no_data_read")
        return []

    # Concatenate all watershed layers
    combined = pd.concat(frames, ignore_index=True)
    combined = gpd.GeoDataFrame(combined, crs=frames[0].crs)

    # Fix and clip
    combined = VectorProcessor.fix_geometries(combined)
    combined = VectorProcessor.clip_to_aoi(combined, AOI_BOUNDS)

    if combined.empty:
        log.warning("cuencas.empty_after_clip")
        return []

    out_path = out_dir / "cuencas.parquet"
    result = VectorProcessor.to_geoparquet(combined, out_path)

    catalog.register({
        "dataset_id": "hydrosheds_cuencas",
        "source": "HydroSHEDS",
        "category": "hidrologia",
        "data_type": "vector",
        "layer": "silver",
        "file_path": str(result),
        "format": "parquet",
        "crs": "EPSG:4326",
        "license": "HydroSHEDS License",
        "ingestor": "processors.vector.cuencas",
        "status": "complete",
        "notes": f"Merged {len(vector_files)} HydroSHEDS layers, clipped to AOI",
    })

    log.info("cuencas.done", rows=len(combined), out=str(result))
    return [result]

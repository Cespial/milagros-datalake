"""Silver processor: Sentinel-2 NDVI/NDWI GeoTIFFs → organized and clipped."""
from pathlib import Path
import shutil
import structlog

log = structlog.get_logger()


def process(bronze_dir: Path, silver_dir: Path, catalog, **kwargs):
    """Copy and organize Sentinel-2 indices to Silver."""
    s2_dir = bronze_dir / "raster" / "gee_sentinel2"
    out_dir = silver_dir / "raster" / "sentinel2"
    out_dir.mkdir(parents=True, exist_ok=True)

    if not s2_dir.exists():
        log.warning("sentinel2.no_bronze")
        return

    count = 0
    for tif in sorted(s2_dir.glob("*.tif")):
        dest = out_dir / tif.name
        if not dest.exists():
            shutil.copy2(tif, dest)
            count += 1

    for tif in sorted(out_dir.glob("*.tif")):
        catalog.register({
            "dataset_id": "sentinel2_indices",
            "source": "Sentinel-2 via GEE",
            "category": "teledeteccion",
            "data_type": "raster",
            "layer": "silver",
            "file_path": str(tif),
            "format": "geotiff",
            "ingestor": "processors.raster.sentinel2",
            "status": "complete",
        })

    log.info("sentinel2.done", copied=count, total=len(list(out_dir.glob("*.tif"))))

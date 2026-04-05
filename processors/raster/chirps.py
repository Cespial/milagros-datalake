"""Silver processor: CHIRPS GeoTIFFs → clipped, reprojected GeoTIFFs.

Reads bronze/raster/chirps/*.tif, clips each to the study AOI, and
reprojects to EPSG:4326. Writes to silver/raster/chirps/.
"""

from pathlib import Path

import structlog

from config.settings import AOI_BOUNDS
from processors.base import RasterProcessor

log = structlog.get_logger()

SOURCE_SUBDIR = "raster/chirps"
OUT_SUBDIR = "raster/chirps"


def process(bronze_dir: Path, silver_dir: Path, catalog, **kwargs) -> list[Path]:
    """Clip and reproject CHIRPS GeoTIFF files."""
    bronze_path = bronze_dir / SOURCE_SUBDIR
    out_dir = silver_dir / OUT_SUBDIR
    out_dir.mkdir(parents=True, exist_ok=True)

    tif_files = sorted(bronze_path.glob("*.tif")) if bronze_path.exists() else []
    tif_files += sorted(bronze_path.glob("*.tiff")) if bronze_path.exists() else []
    tif_files = list(dict.fromkeys(tif_files))  # deduplicate

    if not tif_files:
        log.warning("chirps.no_bronze_files", path=str(bronze_path))
        return []

    written: list[Path] = []

    for tif_file in tif_files:
        out_path = out_dir / tif_file.name
        if out_path.exists():
            log.info("chirps.skip_existing", out=str(out_path))
            written.append(out_path)
            continue

        try:
            result = RasterProcessor.clip_and_reproject(
                src_path=tif_file,
                out_path=out_path,
                bbox=AOI_BOUNDS,
                dst_crs="EPSG:4326",
            )
            written.append(result)

            catalog.register({
                "dataset_id": "chirps",
                "source": "CHIRPS v2.0",
                "category": "meteorologia",
                "data_type": "raster",
                "layer": "silver",
                "file_path": str(result),
                "format": "tif",
                "crs": "EPSG:4326",
                "license": "CC0",
                "ingestor": "processors.raster.chirps",
                "status": "complete",
                "notes": f"Clipped from {tif_file.name}",
            })
        except Exception as exc:
            log.error("chirps.process_failed", file=str(tif_file), error=str(exc))

    log.info("chirps.done", files_written=len(written))
    return written

"""Silver processor: DEM merge — ALOS > Copernicus > SRTM priority mosaic.

Reads bronze/raster/gee_dem/*.tif (expected naming: alos_*.tif, cop30_*.tif, srtm_*.tif).
Merges with priority: ALOS-12.5m (highest res) > Copernicus 30m > SRTM 30m.
Uses rasterio.merge with first-valid-pixel strategy.
Writes merged DEM to silver/raster/dem/merged_dem.tif.
"""

from pathlib import Path

import structlog

from config.settings import AOI_BOUNDS, CRS_WGS84
from processors.base import RasterProcessor

log = structlog.get_logger()

SOURCE_SUBDIR = "raster/gee_dem"
OUT_SUBDIR = "raster/dem"

# Priority order: highest-res first
DEM_PRIORITY = ["alos", "cop30", "srtm"]


def process(bronze_dir: Path, silver_dir: Path, catalog, **kwargs) -> list[Path]:
    """Merge DEM sources with ALOS > Copernicus > SRTM priority."""
    import rasterio
    from rasterio.merge import merge
    from rasterio.warp import calculate_default_transform, reproject, Resampling
    from rasterio.crs import CRS

    bronze_path = bronze_dir / SOURCE_SUBDIR
    out_dir = silver_dir / OUT_SUBDIR
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / "merged_dem.tif"
    if out_path.exists():
        log.info("dem.skip_existing", out=str(out_path))
        return [out_path]

    if not bronze_path.exists():
        log.warning("dem.no_bronze_dir", path=str(bronze_path))
        return []

    # Collect all TIFFs and sort by priority
    all_tifs: list[Path] = sorted(bronze_path.glob("*.tif"))
    all_tifs += sorted(bronze_path.glob("*.tiff"))

    if not all_tifs:
        log.warning("dem.no_tif_files", path=str(bronze_path))
        return []

    def _priority(p: Path) -> int:
        name_lower = p.name.lower()
        for i, prefix in enumerate(DEM_PRIORITY):
            if prefix in name_lower:
                return i
        return len(DEM_PRIORITY)  # lowest priority for unknown

    ordered_tifs = sorted(all_tifs, key=_priority)
    log.info("dem.merge_order", files=[f.name for f in ordered_tifs])

    # Clip each source first, then merge
    west, south, east, north = AOI_BOUNDS

    datasets = []
    opened_paths: list[Path] = []

    try:
        for tif_file in ordered_tifs:
            clipped_path = out_dir / f"_clip_{tif_file.stem}.tif"
            try:
                RasterProcessor.clip_and_reproject(
                    src_path=tif_file,
                    out_path=clipped_path,
                    bbox=AOI_BOUNDS,
                    dst_crs=CRS_WGS84,
                )
                opened_paths.append(clipped_path)
                datasets.append(rasterio.open(str(clipped_path)))
            except Exception as exc:
                log.error("dem.clip_failed", file=str(tif_file), error=str(exc))

        if not datasets:
            log.error("dem.no_datasets_to_merge")
            return []

        merged_data, merged_transform = merge(datasets, method="first")
        meta = datasets[0].meta.copy()
        meta.update(
            {
                "height": merged_data.shape[1],
                "width": merged_data.shape[2],
                "transform": merged_transform,
                "driver": "GTiff",
                "compress": "deflate",
                "crs": CRS.from_string(CRS_WGS84),
            }
        )

        with rasterio.open(str(out_path), "w", **meta) as dst:
            dst.write(merged_data)

        log.info("dem.merged", out=str(out_path), sources=len(datasets))

    finally:
        for ds in datasets:
            ds.close()
        # Clean up intermediate clip files
        for p in opened_paths:
            p.unlink(missing_ok=True)

    catalog.register({
        "dataset_id": "dem_merged",
        "source": "ALOS/Copernicus/SRTM (merged)",
        "category": "geoespacial",
        "data_type": "raster",
        "layer": "silver",
        "file_path": str(out_path),
        "format": "tif",
        "crs": CRS_WGS84,
        "license": "CC0",
        "ingestor": "processors.raster.dem",
        "status": "complete",
        "notes": f"Priority merge: {', '.join(DEM_PRIORITY)}. Sources: {len(ordered_tifs)} files.",
    })

    return [out_path]

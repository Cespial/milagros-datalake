"""Silver processor: ERA5 NetCDF → Cloud-Optimised GeoTIFF (COG).

Reads bronze/raster/cds_era5/*.nc, converts each variable to a COG
clipped to the study AOI, writes to silver/raster/era5/.
"""

from pathlib import Path

import structlog

from config.settings import AOI_BOUNDS
from processors.base import RasterProcessor

log = structlog.get_logger()

SOURCE_SUBDIR = "raster/cds_era5"
OUT_SUBDIR = "raster/era5"

# ERA5 variables expected in the NetCDF files
ERA5_VARIABLES = [
    "total_precipitation",
    "2m_temperature",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "surface_solar_radiation_downwards",
]

# Short variable name mapping (NetCDF internal names may differ)
ERA5_SHORT = {
    "total_precipitation": "tp",
    "2m_temperature": "t2m",
    "10m_u_component_of_wind": "u10",
    "10m_v_component_of_wind": "v10",
    "surface_solar_radiation_downwards": "ssrd",
}


def process(bronze_dir: Path, silver_dir: Path, catalog, **kwargs) -> list[Path]:
    """Convert ERA5 NetCDF files to COG GeoTIFFs."""
    import xarray as xr

    bronze_path = bronze_dir / SOURCE_SUBDIR
    out_dir = silver_dir / OUT_SUBDIR
    out_dir.mkdir(parents=True, exist_ok=True)

    nc_files = sorted(bronze_path.glob("*.nc")) if bronze_path.exists() else []
    if not nc_files:
        log.warning("era5.no_bronze_files", path=str(bronze_path))
        return []

    written: list[Path] = []

    for nc_file in nc_files:
        log.info("era5.open", file=str(nc_file))
        try:
            ds = xr.open_dataset(str(nc_file))
        except Exception as exc:
            log.error("era5.open_failed", file=str(nc_file), error=str(exc))
            continue

        for var in ds.data_vars:
            var_str = str(var)
            out_path = out_dir / f"{nc_file.stem}_{var_str}.tif"

            if out_path.exists():
                log.info("era5.skip_existing", out=str(out_path))
                written.append(out_path)
                continue

            try:
                result = RasterProcessor.netcdf_to_cog(
                    src_path=nc_file,
                    out_path=out_path,
                    variable=var_str,
                    bbox=AOI_BOUNDS,
                )
                written.append(result)

                catalog.register({
                    "dataset_id": f"era5_{var_str}",
                    "source": "Copernicus CDS ERA5",
                    "category": "meteorologia",
                    "data_type": "raster",
                    "layer": "silver",
                    "file_path": str(result),
                    "format": "tif",
                    "crs": "EPSG:4326",
                    "license": "Copernicus License",
                    "ingestor": "processors.raster.era5",
                    "status": "complete",
                    "notes": f"COG from {nc_file.name}, variable={var_str}",
                })
            except Exception as exc:
                log.error("era5.convert_failed", file=str(nc_file), variable=var_str, error=str(exc))

        ds.close()

    log.info("era5.done", files_written=len(written))
    return written

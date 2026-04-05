"""Base processor classes for Bronze → Silver transformations."""

from pathlib import Path
from typing import Any

import structlog

log = structlog.get_logger()


class TabularProcessor:
    """Utilities for cleaning and writing tabular (Parquet) data."""

    @staticmethod
    def clean_nulls(df):
        """Drop columns that are entirely null; fill remaining nulls with sensible defaults."""
        import pandas as pd

        # Drop columns where ALL values are null
        df = df.dropna(axis=1, how="all")

        # Fill numeric nulls with NaN (already the default) and object nulls with empty string
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].fillna("")
        return df

    @staticmethod
    def standardize_columns(df):
        """Lowercase column names, strip whitespace, replace spaces/hyphens with underscores."""
        import re

        df = df.copy()
        df.columns = [
            re.sub(r"[\s\-]+", "_", col.strip().lower())
            for col in df.columns
        ]
        return df

    @staticmethod
    def write_partitioned(df, out_dir: Path, partition_col: str | None = None) -> list[Path]:
        """Write DataFrame to Parquet, optionally partitioned by a column.

        Returns list of written file paths.
        """
        import pyarrow as pa
        import pyarrow.parquet as pq

        out_dir.mkdir(parents=True, exist_ok=True)

        if partition_col and partition_col in df.columns:
            paths: list[Path] = []
            for value, group in df.groupby(partition_col):
                safe_val = str(value).replace("/", "_").replace(" ", "_")
                part_dir = out_dir / f"{partition_col}={safe_val}"
                part_dir.mkdir(parents=True, exist_ok=True)
                out_path = part_dir / "data.parquet"
                table = pa.Table.from_pandas(group, preserve_index=False)
                pq.write_table(table, str(out_path))
                log.info("tabular.write_partition", path=str(out_path), rows=len(group))
                paths.append(out_path)
            return paths
        else:
            out_path = out_dir / "data.parquet"
            table = pa.Table.from_pandas(df, preserve_index=False)
            pq.write_table(table, str(out_path))
            log.info("tabular.write", path=str(out_path), rows=len(df))
            return [out_path]


class VectorProcessor:
    """Utilities for cleaning and writing vector (GeoParquet) data."""

    @staticmethod
    def fix_geometries(gdf):
        """Fix invalid geometries using buffer(0) trick."""
        from shapely.validation import make_valid

        gdf = gdf.copy()
        invalid = ~gdf.geometry.is_valid
        if invalid.any():
            log.info("vector.fix_geometries", invalid_count=int(invalid.sum()))
            gdf.loc[invalid, "geometry"] = gdf.loc[invalid, "geometry"].apply(make_valid)
        return gdf

    @staticmethod
    def clip_to_aoi(gdf, bbox: tuple[float, float, float, float]):
        """Clip GeoDataFrame to bounding box (west, south, east, north)."""
        import geopandas as gpd
        from shapely.geometry import box

        west, south, east, north = bbox
        aoi_geom = box(west, south, east, north)
        aoi_gdf = gpd.GeoDataFrame(geometry=[aoi_geom], crs="EPSG:4326")

        # Reproject to WGS84 if needed
        if gdf.crs is not None and gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs("EPSG:4326")

        clipped = gpd.clip(gdf, aoi_gdf)
        log.info("vector.clip_to_aoi", before=len(gdf), after=len(clipped), bbox=bbox)
        return clipped

    @staticmethod
    def to_geoparquet(gdf, out_path: Path) -> Path:
        """Write GeoDataFrame to GeoParquet."""
        out_path.parent.mkdir(parents=True, exist_ok=True)
        gdf.to_parquet(str(out_path))
        log.info("vector.write_geoparquet", path=str(out_path), rows=len(gdf))
        return out_path


class RasterProcessor:
    """Utilities for reprojecting and converting raster data."""

    @staticmethod
    def clip_and_reproject(
        src_path: Path,
        out_path: Path,
        bbox: tuple[float, float, float, float],
        dst_crs: str = "EPSG:4326",
    ) -> Path:
        """Clip GeoTIFF to bbox and reproject to dst_crs. Writes a COG-compatible GeoTIFF."""
        import numpy as np
        import rasterio
        from rasterio.crs import CRS
        from rasterio.mask import mask
        from rasterio.warp import calculate_default_transform, reproject, Resampling
        from shapely.geometry import box, mapping

        west, south, east, north = bbox
        aoi_geom = box(west, south, east, north)

        out_path.parent.mkdir(parents=True, exist_ok=True)

        with rasterio.open(str(src_path)) as src:
            dst_crs_obj = CRS.from_string(dst_crs)

            # Reproject source to dst_crs first (in memory)
            transform, width, height = calculate_default_transform(
                src.crs, dst_crs_obj, src.width, src.height, *src.bounds
            )
            kwargs = src.meta.copy()
            kwargs.update({"crs": dst_crs_obj, "transform": transform, "width": width, "height": height})

            import tempfile
            with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as tmp:
                tmp_path = Path(tmp.name)

            with rasterio.open(str(tmp_path), "w", **kwargs) as dst:
                for i in range(1, src.count + 1):
                    reproject(
                        source=rasterio.band(src, i),
                        destination=rasterio.band(dst, i),
                        src_transform=src.transform,
                        src_crs=src.crs,
                        dst_transform=transform,
                        dst_crs=dst_crs_obj,
                        resampling=Resampling.nearest,
                    )

            # Now clip to AOI
            with rasterio.open(str(tmp_path)) as reprojected:
                clipped_data, clipped_transform = mask(
                    reprojected, [mapping(aoi_geom)], crop=True
                )
                clip_meta = reprojected.meta.copy()
                clip_meta.update(
                    {
                        "height": clipped_data.shape[1],
                        "width": clipped_data.shape[2],
                        "transform": clipped_transform,
                        "driver": "GTiff",
                        "compress": "deflate",
                    }
                )
                with rasterio.open(str(out_path), "w", **clip_meta) as out_ds:
                    out_ds.write(clipped_data)

            tmp_path.unlink(missing_ok=True)

        log.info("raster.clip_reproject", src=str(src_path), out=str(out_path))
        return out_path

    @staticmethod
    def netcdf_to_cog(
        src_path: Path,
        out_path: Path,
        variable: str,
        bbox: tuple[float, float, float, float] | None = None,
        dst_crs: str = "EPSG:4326",
    ) -> Path:
        """Convert a NetCDF variable to Cloud-Optimised GeoTIFF (COG).

        Writes one band per time step if the variable has a time dimension,
        otherwise writes a single-band COG.
        """
        import numpy as np
        import rioxarray  # noqa: F401 — registers rio accessor on xarray
        import xarray as xr
        import rasterio
        from rasterio.crs import CRS

        out_path.parent.mkdir(parents=True, exist_ok=True)

        ds = xr.open_dataset(str(src_path))
        da = ds[variable]

        # Assign CRS via rioxarray
        da = da.rio.write_crs(dst_crs)

        # Detect spatial dimensions
        x_dim = next((d for d in da.dims if d.lower() in ("lon", "longitude", "x")), None)
        y_dim = next((d for d in da.dims if d.lower() in ("lat", "latitude", "y")), None)
        if x_dim:
            da = da.rio.set_spatial_dims(x_dim=x_dim, y_dim=y_dim)

        # Clip to bbox if provided
        if bbox:
            west, south, east, north = bbox
            da = da.rio.clip_box(minx=west, miny=south, maxx=east, maxy=north)

        # Reproject if needed
        da = da.rio.reproject(dst_crs)

        # Write to COG
        da.rio.to_raster(str(out_path), driver="GTiff", compress="deflate")

        log.info("raster.netcdf_to_cog", src=str(src_path), variable=variable, out=str(out_path))
        ds.close()
        return out_path

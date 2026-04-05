"""Base processor classes for tabular, raster, and vector data."""

import re
from pathlib import Path

import pandas as pd
import geopandas as gpd
from shapely import make_valid
import structlog

from config.settings import AOI_BOUNDS, CRS_WGS84
from catalog.manager import CatalogManager

log = structlog.get_logger()

NULL_SENTINELS = {"-999", "-999.0", "-9999", "N/A", "ND", "NA", "null", "NULL", ""}


class TabularProcessor:
    @staticmethod
    def clean_nulls(df: pd.DataFrame) -> pd.DataFrame:
        return df.replace(NULL_SENTINELS, pd.NA).replace(
            {-999: pd.NA, -999.0: pd.NA, -9999: pd.NA, -9999.0: pd.NA}
        )

    @staticmethod
    def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
        def to_snake(name: str) -> str:
            name = name.strip()
            name = re.sub(r"[()/%°#]", "_", name)
            name = re.sub(r"[\s\-\.]+", "_", name)
            name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
            name = re.sub(r"([a-z])([A-Z])", r"\1_\2", name)
            name = re.sub(r"([0-9])([a-zA-Z])", r"\1_\2", name)
            name = re.sub(r"_+", "_", name)
            return name.lower().strip("_")
        df.columns = [to_snake(c) for c in df.columns]
        return df

    @staticmethod
    def write_partitioned(df: pd.DataFrame, base_dir: Path, date_col: str = "fecha"):
        if date_col in df.columns:
            df["year"] = pd.to_datetime(df[date_col]).dt.year
        elif "timestamp" in df.columns:
            df["year"] = pd.to_datetime(df["timestamp"]).dt.year
        else:
            df["year"] = 0
        for year, group in df.groupby("year"):
            year_dir = base_dir / f"year={year}"
            year_dir.mkdir(parents=True, exist_ok=True)
            out = year_dir / "data.parquet"
            group.drop(columns=["year"]).to_parquet(out, index=False)
            log.info("processor.wrote_partition", year=year, rows=len(group), path=str(out))


class VectorProcessor:
    @staticmethod
    def clip_to_aoi(gdf: gpd.GeoDataFrame, aoi_bounds: tuple = AOI_BOUNDS) -> gpd.GeoDataFrame:
        from shapely.geometry import box
        aoi_box = box(*aoi_bounds)
        return gdf.clip(aoi_box)

    @staticmethod
    def fix_geometries(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        gdf = gdf.copy()
        gdf["geometry"] = gdf.geometry.apply(make_valid)
        return gdf

    @staticmethod
    def to_geoparquet(gdf: gpd.GeoDataFrame, out_path: Path, crs: str = CRS_WGS84):
        if gdf.crs is None:
            gdf = gdf.set_crs(crs)
        elif str(gdf.crs) != crs:
            gdf = gdf.to_crs(crs)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        gdf.to_parquet(out_path)
        log.info("processor.wrote_geoparquet", path=str(out_path), rows=len(gdf))


class RasterProcessor:
    @staticmethod
    def clip_and_reproject(input_path: Path, output_path: Path, aoi_bounds: tuple = AOI_BOUNDS):
        import rasterio
        from rasterio.mask import mask
        from shapely.geometry import box
        aoi_geom = [box(*aoi_bounds).__geo_interface__]
        with rasterio.open(input_path) as src:
            out_image, out_transform = mask(src, aoi_geom, crop=True)
            out_meta = src.meta.copy()
            out_meta.update({
                "driver": "GTiff",
                "height": out_image.shape[1],
                "width": out_image.shape[2],
                "transform": out_transform,
                "crs": CRS_WGS84,
            })
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with rasterio.open(output_path, "w", **out_meta) as dest:
            dest.write(out_image)
        log.info("processor.clipped_raster", input=str(input_path), output=str(output_path))

    @staticmethod
    def netcdf_to_cog(input_path: Path, output_dir: Path, variable: str):
        import xarray as xr
        import rioxarray
        ds = xr.open_dataset(input_path)
        da = ds[variable]
        if "time" in da.dims:
            for t in range(len(da.time)):
                time_val = pd.Timestamp(da.time.values[t])
                out_path = output_dir / f"{variable}_{time_val.strftime('%Y%m')}.tif"
                if out_path.exists():
                    continue
                slice_da = da.isel(time=t)
                slice_da.rio.set_crs("EPSG:4326")
                out_path.parent.mkdir(parents=True, exist_ok=True)
                slice_da.rio.to_raster(out_path, driver="COG")
        else:
            out_path = output_dir / f"{variable}.tif"
            da.rio.set_crs("EPSG:4326")
            out_path.parent.mkdir(parents=True, exist_ok=True)
            da.rio.to_raster(out_path, driver="COG")
        ds.close()
        log.info("processor.netcdf_to_cog", variable=variable, output_dir=str(output_dir))

"""Tests for base processors."""

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import pytest

from processors.base import TabularProcessor, VectorProcessor


def test_tabular_clean_nulls():
    df = pd.DataFrame({
        "caudal": [1.5, -999, "N/A", "ND", None, 3.2],
        "nivel": [0.5, 0.8, "-999.0", "", 1.0, 1.2],
    })
    cleaned = TabularProcessor.clean_nulls(df)
    # None (row 4) is inherently NA in pandas object dtype; sentinels -999, "N/A", "ND" add 3 more
    assert cleaned["caudal"].isna().sum() == 4
    assert cleaned["nivel"].isna().sum() == 2


def test_tabular_standardize_columns():
    df = pd.DataFrame({"Caudal Medio (m3/s)": [1], "  Nivel  ": [2], "TEMP": [3]})
    result = TabularProcessor.standardize_columns(df)
    assert list(result.columns) == ["caudal_medio_m3_s", "nivel", "temp"]


def test_vector_clip_to_aoi():
    gdf = gpd.GeoDataFrame(
        {"name": ["inside", "outside"]},
        geometry=[Point(-75.5, 6.4), Point(-80, 10)],
        crs="EPSG:4326",
    )
    from config.settings import AOI_BOUNDS
    clipped = VectorProcessor.clip_to_aoi(gdf, AOI_BOUNDS)
    assert len(clipped) == 1
    assert clipped.iloc[0]["name"] == "inside"


def test_vector_fix_geometries():
    from shapely.geometry import Polygon
    bowtie = Polygon([(0, 0), (1, 1), (1, 0), (0, 1)])
    gdf = gpd.GeoDataFrame({"name": ["bowtie"]}, geometry=[bowtie], crs="EPSG:4326")
    assert not gdf.geometry.is_valid.all()
    fixed = VectorProcessor.fix_geometries(gdf)
    assert fixed.geometry.is_valid.all()

"""Shared test fixtures."""

import tempfile
from pathlib import Path

import pytest

from catalog.manager import CatalogManager


@pytest.fixture
def tmp_catalog(tmp_path):
    """CatalogManager backed by a temporary DuckDB file."""
    db_path = tmp_path / "test_catalog.duckdb"
    mgr = CatalogManager(db_path)
    return mgr


@pytest.fixture
def sample_metadata():
    """Minimal valid metadata dict for catalog registration."""
    return {
        "dataset_id": "test_dataset_001",
        "source": "Test Source",
        "category": "hidrologia",
        "data_type": "tabular",
        "layer": "bronze",
        "file_path": "bronze/tabular/test/data.csv",
        "format": "csv",
        "temporal_start": "2020-01-01",
        "temporal_end": "2024-12-31",
        "temporal_resolution": "daily",
        "spatial_bbox": "[-75.8,6.25,-75.25,6.7]",
        "spatial_resolution": "station",
        "crs": "EPSG:4326",
        "variables": ["caudal_m3s", "nivel_m"],
        "license": "CC0",
        "ingestor": "test_ingestor",
        "status": "complete",
        "notes": "",
    }

"""Tests for CatalogManager."""

import pytest
from catalog.manager import CatalogManager


def test_init_creates_table(tmp_catalog):
    """CatalogManager creates the datasets table on init."""
    result = tmp_catalog.query("SELECT count(*) as n FROM datasets")
    assert result[0]["n"] == 0


def test_register_and_query(tmp_catalog, sample_metadata):
    """register() inserts a row, query() retrieves it."""
    tmp_catalog.register(sample_metadata)
    rows = tmp_catalog.query(
        "SELECT dataset_id, source FROM datasets WHERE dataset_id = ?",
        [sample_metadata["dataset_id"]],
    )
    assert len(rows) == 1
    assert rows[0]["dataset_id"] == "test_dataset_001"
    assert rows[0]["source"] == "Test Source"


def test_register_sets_ingested_at(tmp_catalog, sample_metadata):
    """register() auto-sets ingested_at timestamp."""
    tmp_catalog.register(sample_metadata)
    rows = tmp_catalog.query(
        "SELECT ingested_at FROM datasets WHERE dataset_id = ?",
        [sample_metadata["dataset_id"]],
    )
    assert rows[0]["ingested_at"] is not None


def test_register_computes_file_hash(tmp_catalog, sample_metadata, tmp_path):
    """register() computes file hash if the file exists."""
    test_file = tmp_path / "data.csv"
    test_file.write_text("col1,col2\n1,2\n")
    sample_metadata["file_path"] = str(test_file)
    tmp_catalog.register(sample_metadata)
    rows = tmp_catalog.query(
        "SELECT file_hash, file_size_mb FROM datasets WHERE dataset_id = ?",
        [sample_metadata["dataset_id"]],
    )
    assert rows[0]["file_hash"] is not None
    assert rows[0]["file_size_mb"] > 0


def test_list_by_category(tmp_catalog, sample_metadata):
    """list_datasets() filters by category."""
    tmp_catalog.register(sample_metadata)
    meta2 = {**sample_metadata, "dataset_id": "other_001", "category": "geologia"}
    tmp_catalog.register(meta2)
    hydro = tmp_catalog.list_datasets(category="hidrologia")
    assert len(hydro) == 1
    assert hydro[0]["dataset_id"] == "test_dataset_001"


def test_list_by_layer(tmp_catalog, sample_metadata):
    """list_datasets() filters by layer."""
    tmp_catalog.register(sample_metadata)
    meta2 = {**sample_metadata, "dataset_id": "silver_001", "layer": "silver"}
    tmp_catalog.register(meta2)
    bronze = tmp_catalog.list_datasets(layer="bronze")
    assert len(bronze) == 1


def test_list_all(tmp_catalog, sample_metadata):
    """list_datasets() with no filters returns all."""
    tmp_catalog.register(sample_metadata)
    all_ds = tmp_catalog.list_datasets()
    assert len(all_ds) == 1


def test_get_lineage(tmp_catalog, sample_metadata):
    """get_lineage() returns entries for a dataset across layers."""
    tmp_catalog.register(sample_metadata)
    silver = {
        **sample_metadata,
        "dataset_id": "test_dataset_001",
        "layer": "silver",
        "file_path": "silver/tabular/hidrologia/year=2024/data.parquet",
        "format": "parquet",
    }
    tmp_catalog.register(silver)
    lineage = tmp_catalog.get_lineage("test_dataset_001")
    assert len(lineage) == 2
    layers = {r["layer"] for r in lineage}
    assert layers == {"bronze", "silver"}

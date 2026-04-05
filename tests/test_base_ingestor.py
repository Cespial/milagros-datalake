"""Tests for BaseIngestor ABC."""

from pathlib import Path

import pytest

from ingestors.base import BaseIngestor


class DummyIngestor(BaseIngestor):
    """Concrete implementation for testing."""

    name = "dummy"
    source_type = "api"
    data_type = "tabular"
    category = "hidrologia"
    schedule = "daily"
    license = "CC0"

    def fetch(self, **kwargs) -> list[Path]:
        out = self.bronze_dir / "test.csv"
        out.write_text("a,b\n1,2\n")
        return [out]


def test_ingestor_creates_bronze_dir(tmp_catalog, tmp_path):
    """Ingestor creates its bronze subdirectory."""
    bronze = tmp_path / "bronze"
    ing = DummyIngestor(catalog=tmp_catalog, bronze_root=bronze)
    assert ing.bronze_dir.exists()
    assert ing.bronze_dir == bronze / "tabular" / "dummy"


def test_fetch_returns_paths(tmp_catalog, tmp_path):
    """fetch() returns list of created file paths."""
    bronze = tmp_path / "bronze"
    ing = DummyIngestor(catalog=tmp_catalog, bronze_root=bronze)
    paths = ing.fetch()
    assert len(paths) == 1
    assert paths[0].exists()
    assert paths[0].read_text() == "a,b\n1,2\n"


def test_run_fetches_and_registers(tmp_catalog, tmp_path):
    """run() calls fetch then registers in catalog."""
    bronze = tmp_path / "bronze"
    ing = DummyIngestor(catalog=tmp_catalog, bronze_root=bronze)
    ing.run()
    rows = tmp_catalog.list_datasets()
    assert len(rows) == 1
    assert rows[0]["dataset_id"] == "dummy"
    assert rows[0]["source"] == "dummy"
    assert rows[0]["status"] == "complete"


def test_run_records_failure(tmp_catalog, tmp_path):
    """run() records failure status when fetch raises."""

    class FailIngestor(BaseIngestor):
        name = "fail"
        source_type = "api"
        data_type = "tabular"
        category = "hidrologia"
        schedule = "daily"
        license = "CC0"

        def fetch(self, **kwargs):
            raise ConnectionError("API down")

    bronze = tmp_path / "bronze"
    ing = FailIngestor(catalog=tmp_catalog, bronze_root=bronze)
    ing.run()
    rows = tmp_catalog.list_datasets()
    assert len(rows) == 1
    assert rows[0]["status"] == "failed"

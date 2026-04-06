"""Tests for CHELSA v2.1 ingestor."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ingestors.chelsa import ChelsaIngestor, CHELSA_VARS


@pytest.fixture
def chelsa_ingestor(tmp_catalog, tmp_path):
    bronze = tmp_path / "bronze"
    return ChelsaIngestor(catalog=tmp_catalog, bronze_root=bronze)


@patch("ingestors.chelsa.httpx")
@patch("ingestors.chelsa.ee")
def test_fetch_returns_two_tifs_on_success(mock_ee, mock_httpx, chelsa_ingestor):
    """Happy path: GEE returns valid bytes for both pr and tas."""
    mock_ee.Initialize = MagicMock()
    mock_ee.Geometry.BBox = MagicMock()

    # Fake image / collection chain
    mock_img = MagicMock()
    mock_img.getDownloadURL.return_value = "https://example.com/fake.tif"
    mock_col = MagicMock()
    mock_col.select.return_value.mean.return_value.clip.return_value = mock_img
    mock_ee.ImageCollection.return_value = mock_col

    # Fake HTTP response
    fake_response = MagicMock()
    fake_response.content = b"GIF89a"  # small sentinel bytes
    fake_response.raise_for_status = MagicMock()
    mock_httpx.get.return_value = fake_response

    paths = chelsa_ingestor.fetch()

    assert len(paths) == len(CHELSA_VARS)
    for path in paths:
        assert path.suffix == ".tif"
        assert path.exists()


@patch("ingestors.chelsa.ee")
def test_fetch_fallback_metadata_on_gee_failure(mock_ee, chelsa_ingestor):
    """When GEE raises, a fallback JSON metadata file is returned."""
    mock_ee.Initialize = MagicMock()
    mock_ee.Geometry.BBox = MagicMock()
    mock_ee.ImageCollection.side_effect = Exception("GEE asset not found")

    paths = chelsa_ingestor.fetch()

    assert len(paths) == 1
    meta_path = paths[0]
    assert meta_path.suffix == ".json"
    assert meta_path.exists()

    meta = json.loads(meta_path.read_text())
    assert "CHELSA" in meta["source"]
    assert "url" in meta
    assert "aoi_climate_estimate" in meta


@patch("ingestors.chelsa.httpx")
@patch("ingestors.chelsa.ee")
def test_fetch_skips_existing_files(mock_ee, mock_httpx, chelsa_ingestor):
    """Already-downloaded .tif files are skipped; httpx is not called."""
    mock_ee.Initialize = MagicMock()
    mock_ee.Geometry.BBox = MagicMock()
    mock_ee.ImageCollection = MagicMock()

    # Pre-populate all expected output files
    chelsa_ingestor.bronze_dir.mkdir(parents=True, exist_ok=True)
    for _, label in CHELSA_VARS:
        (chelsa_ingestor.bronze_dir / f"chelsa_{label}_annual_mean.tif").write_bytes(b"dummy")

    paths = chelsa_ingestor.fetch()

    assert len(paths) == len(CHELSA_VARS)
    mock_httpx.get.assert_not_called()


def test_ingestor_metadata():
    """Verify class-level metadata attributes are correctly set."""
    assert ChelsaIngestor.name == "chelsa"
    assert ChelsaIngestor.source_type == "gee"
    assert ChelsaIngestor.data_type == "raster"
    assert ChelsaIngestor.category == "meteorologia"
    assert ChelsaIngestor.schedule == "once"
    assert "CC-BY" in ChelsaIngestor.license

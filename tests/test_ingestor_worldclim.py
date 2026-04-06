"""Tests for WorldClim v2.1 bioclimatic ingestor."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ingestors.worldclim import WorldClimIngestor, BIO_VARS


@pytest.fixture
def worldclim_ingestor(tmp_catalog, tmp_path):
    bronze = tmp_path / "bronze"
    return WorldClimIngestor(catalog=tmp_catalog, bronze_root=bronze)


@patch("ingestors.worldclim.httpx")
@patch("ingestors.worldclim.ee")
def test_fetch_returns_all_bio_vars(mock_ee, mock_httpx, worldclim_ingestor):
    """Happy path: downloads one GeoTIFF per BIO variable."""
    mock_ee.Initialize = MagicMock()
    mock_ee.Geometry.BBox = MagicMock()

    mock_img = MagicMock()
    mock_img.getDownloadURL.return_value = "https://example.com/fake.tif"
    mock_ee.Image.return_value.select.return_value.clip.return_value = mock_img

    fake_response = MagicMock()
    fake_response.content = b"TIFF_BYTES"
    fake_response.raise_for_status = MagicMock()
    mock_httpx.get.return_value = fake_response

    paths = worldclim_ingestor.fetch()

    assert len(paths) == len(BIO_VARS)
    for path in paths:
        assert path.suffix == ".tif"
        assert path.exists()


@patch("ingestors.worldclim.httpx")
@patch("ingestors.worldclim.ee")
def test_fetch_skips_failed_vars_and_continues(mock_ee, mock_httpx, worldclim_ingestor):
    """If one variable download fails, the rest still complete."""
    mock_ee.Initialize = MagicMock()
    mock_ee.Geometry.BBox = MagicMock()

    call_count = 0

    def fake_clip(aoi):
        nonlocal call_count
        call_count += 1
        m = MagicMock()
        if call_count == 1:
            m.getDownloadURL.side_effect = Exception("network timeout")
        else:
            m.getDownloadURL.return_value = "https://example.com/fake.tif"
        return m

    mock_ee.Image.return_value.select.return_value.clip.side_effect = fake_clip

    fake_response = MagicMock()
    fake_response.content = b"TIFF_BYTES"
    fake_response.raise_for_status = MagicMock()
    mock_httpx.get.return_value = fake_response

    paths = worldclim_ingestor.fetch()

    # First var failed, rest succeeded
    assert len(paths) == len(BIO_VARS) - 1


@patch("ingestors.worldclim.httpx")
@patch("ingestors.worldclim.ee")
def test_fetch_skips_existing_files(mock_ee, mock_httpx, worldclim_ingestor):
    """Pre-existing .tif files are returned immediately without HTTP calls."""
    mock_ee.Initialize = MagicMock()
    mock_ee.Geometry.BBox = MagicMock()
    mock_ee.Image = MagicMock()

    worldclim_ingestor.bronze_dir.mkdir(parents=True, exist_ok=True)
    for var_id in BIO_VARS:
        (worldclim_ingestor.bronze_dir / f"worldclim_{var_id}.tif").write_bytes(b"dummy")

    paths = worldclim_ingestor.fetch()

    assert len(paths) == len(BIO_VARS)
    mock_httpx.get.assert_not_called()


def test_ingestor_metadata():
    """Verify class-level metadata attributes are correctly set."""
    assert WorldClimIngestor.name == "worldclim"
    assert WorldClimIngestor.source_type == "gee"
    assert WorldClimIngestor.data_type == "raster"
    assert WorldClimIngestor.category == "meteorologia"
    assert WorldClimIngestor.schedule == "once"
    assert "CC-BY" in WorldClimIngestor.license

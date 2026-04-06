"""Tests for GEE Sentinel-2 spectral indices ingestor."""

from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from ingestors.gee_sentinel2 import GeeSentinel2Ingestor


@pytest.fixture
def s2_ingestor(tmp_catalog, tmp_path):
    bronze = tmp_path / "bronze"
    return GeeSentinel2Ingestor(catalog=tmp_catalog, bronze_root=bronze)


@patch("ingestors.gee_sentinel2.httpx")
@patch("ingestors.gee_sentinel2.ee")
def test_fetch_single_year_downloads_ndvi_and_ndwi(mock_ee, mock_httpx, s2_ingestor):
    """fetch() for one year produces two GeoTIFFs: ndvi_{year}.tif and ndwi_{year}.tif."""
    # Mock GEE chain
    mock_ee.Initialize = MagicMock()
    mock_aoi = MagicMock()
    mock_ee.Geometry.BBox.return_value = mock_aoi

    mock_col = MagicMock()
    mock_ee.ImageCollection.return_value = mock_col
    mock_col.filterBounds.return_value = mock_col
    mock_col.filterDate.return_value = mock_col
    mock_col.filter.return_value = mock_col
    mock_col.size.return_value.getInfo.return_value = 42
    mock_col.median.return_value.clip.return_value = MagicMock()

    mock_ee.Filter.lt = MagicMock()

    mock_image = mock_col.median.return_value.clip.return_value
    mock_ndvi = MagicMock()
    mock_ndwi = MagicMock()
    mock_image.normalizedDifference.return_value.rename.side_effect = [mock_ndvi, mock_ndwi]
    mock_ndvi.getDownloadURL.return_value = "http://fake-url/ndvi.tif"
    mock_ndwi.getDownloadURL.return_value = "http://fake-url/ndwi.tif"

    # Mock HTTP response with fake GeoTIFF bytes
    fake_response = MagicMock()
    fake_response.content = b"FAKE_GEOTIFF_BYTES"
    fake_response.raise_for_status = MagicMock()
    mock_httpx.get.return_value = fake_response

    paths = s2_ingestor.fetch(start_year=2023, end_year=2023)

    assert len(paths) == 2
    stems = {p.stem for p in paths}
    assert stems == {"ndvi_2023", "ndwi_2023"}
    for p in paths:
        assert p.suffix == ".tif"
        assert p.exists()


@patch("ingestors.gee_sentinel2.httpx")
@patch("ingestors.gee_sentinel2.ee")
def test_fetch_skips_existing_files(mock_ee, mock_httpx, s2_ingestor):
    """fetch() skips years where both NDVI and NDWI files already exist."""
    mock_ee.Initialize = MagicMock()
    mock_ee.Geometry.BBox = MagicMock()

    # Pre-create both output files
    s2_ingestor.bronze_dir.mkdir(parents=True, exist_ok=True)
    ndvi_path = s2_ingestor.bronze_dir / "ndvi_2022.tif"
    ndwi_path = s2_ingestor.bronze_dir / "ndwi_2022.tif"
    ndvi_path.write_bytes(b"existing-ndvi")
    ndwi_path.write_bytes(b"existing-ndwi")

    paths = s2_ingestor.fetch(start_year=2022, end_year=2022)

    assert len(paths) == 2
    # GEE should not have been called at all
    mock_ee.ImageCollection.assert_not_called()
    mock_httpx.get.assert_not_called()


@patch("ingestors.gee_sentinel2.httpx")
@patch("ingestors.gee_sentinel2.ee")
def test_fetch_skips_year_with_no_images(mock_ee, mock_httpx, s2_ingestor):
    """fetch() skips a year when the collection contains zero images."""
    mock_ee.Initialize = MagicMock()
    mock_ee.Geometry.BBox = MagicMock()
    mock_ee.Filter.lt = MagicMock()

    mock_col = MagicMock()
    mock_ee.ImageCollection.return_value = mock_col
    mock_col.filterBounds.return_value = mock_col
    mock_col.filterDate.return_value = mock_col
    mock_col.filter.return_value = mock_col
    mock_col.size.return_value.getInfo.return_value = 0  # no images

    paths = s2_ingestor.fetch(start_year=2017, end_year=2017)

    assert paths == []
    mock_httpx.get.assert_not_called()


@patch("ingestors.gee_sentinel2.httpx")
@patch("ingestors.gee_sentinel2.ee")
def test_fetch_multiple_years(mock_ee, mock_httpx, s2_ingestor):
    """fetch() returns 2 files per year across a multi-year range."""
    mock_ee.Initialize = MagicMock()
    mock_ee.Geometry.BBox = MagicMock()
    mock_ee.Filter.lt = MagicMock()

    mock_col = MagicMock()
    mock_ee.ImageCollection.return_value = mock_col
    mock_col.filterBounds.return_value = mock_col
    mock_col.filterDate.return_value = mock_col
    mock_col.filter.return_value = mock_col
    mock_col.size.return_value.getInfo.return_value = 10
    mock_col.median.return_value.clip.return_value = MagicMock()

    mock_image = mock_col.median.return_value.clip.return_value
    mock_index = MagicMock()
    mock_index.rename.return_value = mock_index
    mock_index.getDownloadURL.return_value = "http://fake-url/index.tif"
    mock_image.normalizedDifference.return_value = mock_index

    fake_response = MagicMock()
    fake_response.content = b"FAKE_GEOTIFF"
    fake_response.raise_for_status = MagicMock()
    mock_httpx.get.return_value = fake_response

    paths = s2_ingestor.fetch(start_year=2021, end_year=2023)

    assert len(paths) == 6  # 3 years x 2 indices
    years_found = {p.stem.split("_")[1] for p in paths}
    assert years_found == {"2021", "2022", "2023"}

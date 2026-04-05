"""Tests for GEE DEM ingestor."""

from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from ingestors.gee_dem import GeeDemIngestor


@pytest.fixture
def dem_ingestor(tmp_catalog, tmp_path):
    bronze = tmp_path / "bronze"
    return GeeDemIngestor(catalog=tmp_catalog, bronze_root=bronze)


@patch("ingestors.gee_dem.ee")
def test_fetch_exports_three_dems(mock_ee, dem_ingestor, tmp_path):
    mock_ee.Initialize = MagicMock()
    mock_ee.Image = MagicMock()
    mock_ee.Geometry.BBox = MagicMock()

    mock_task = MagicMock()
    mock_task.status.return_value = {"state": "COMPLETED"}
    mock_ee.batch.Export.image.toDrive.return_value = mock_task

    for name in ["copernicus_glo30", "srtm_30m", "alos_palsar_12m"]:
        (dem_ingestor.bronze_dir / f"{name}.tif").write_bytes(b"fake-geotiff")

    paths = dem_ingestor.fetch()
    assert len(paths) == 3
    names = {p.stem for p in paths}
    assert names == {"copernicus_glo30", "srtm_30m", "alos_palsar_12m"}

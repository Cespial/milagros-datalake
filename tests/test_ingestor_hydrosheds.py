"""Tests for HydroSHEDS ingestor."""

import zipfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from ingestors.hydrosheds import HydroShedsIngestor


@pytest.fixture
def hydro_ingestor(tmp_catalog, tmp_path):
    bronze = tmp_path / "bronze"
    return HydroShedsIngestor(catalog=tmp_catalog, bronze_root=bronze)


def _create_fake_shapefile(tmp_path, name):
    zip_path = tmp_path / f"{name}.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr(f"{name}.shp", b"fake")
        zf.writestr(f"{name}.shx", b"fake")
        zf.writestr(f"{name}.dbf", b"fake")
        zf.writestr(f"{name}.prj", b"fake")
    return zip_path


@patch("ingestors.hydrosheds.httpx.stream")
def test_fetch_downloads_and_extracts(mock_stream, hydro_ingestor, tmp_path):
    fake_zip = _create_fake_shapefile(tmp_path, "hybas_sa_lev06_v1c")
    zip_bytes = fake_zip.read_bytes()

    mock_response = MagicMock()
    mock_response.__enter__ = MagicMock(return_value=mock_response)
    mock_response.__exit__ = MagicMock(return_value=False)
    mock_response.iter_bytes = MagicMock(return_value=iter([zip_bytes]))
    mock_response.raise_for_status = MagicMock()
    mock_stream.return_value = mock_response

    paths = hydro_ingestor.fetch()
    assert len(paths) >= 1

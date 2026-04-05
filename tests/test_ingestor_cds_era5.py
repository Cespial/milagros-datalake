"""Tests for CDS ERA5-Land ingestor."""

from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from ingestors.cds_era5 import CdsEra5Ingestor


@pytest.fixture
def era5_ingestor(tmp_catalog, tmp_path):
    bronze = tmp_path / "bronze"
    return CdsEra5Ingestor(catalog=tmp_catalog, bronze_root=bronze)


@patch("ingestors.cds_era5.cdsapi.Client")
def test_fetch_creates_nc_files(mock_client_cls, era5_ingestor):
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client

    def fake_retrieve(dataset, request, target):
        Path(target).write_bytes(b"fake-netcdf-data")

    mock_client.retrieve.side_effect = fake_retrieve

    paths = era5_ingestor.fetch(start_year=2024, end_year=2024, months=[1])
    assert len(paths) == 1
    assert paths[0].suffix == ".nc"
    assert paths[0].exists()
    mock_client.retrieve.assert_called_once()


@patch("ingestors.cds_era5.cdsapi.Client")
def test_fetch_request_has_correct_area(mock_client_cls, era5_ingestor):
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client

    def fake_retrieve(dataset, request, target):
        Path(target).write_bytes(b"fake")
        assert "area" in request
        area = request["area"]
        assert area[0] > area[2]  # north > south

    mock_client.retrieve.side_effect = fake_retrieve
    era5_ingestor.fetch(start_year=2024, end_year=2024, months=[1])

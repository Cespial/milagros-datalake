"""Tests for NASA POWER ingestor."""

import json
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from ingestors.nasa_power import NasaPowerIngestor


@pytest.fixture
def nasa_ingestor(tmp_catalog, tmp_path):
    bronze = tmp_path / "bronze"
    return NasaPowerIngestor(catalog=tmp_catalog, bronze_root=bronze)


def _mock_response():
    """Fake NASA POWER API response."""
    return {
        "properties": {
            "parameter": {
                "T2M": {"20240101": 18.5, "20240102": 19.1},
                "PRECTOTCORR": {"20240101": 2.3, "20240102": 0.0},
                "ALLSKY_SFC_SW_DWN": {"20240101": 4.8, "20240102": 5.2},
                "WS2M": {"20240101": 1.2, "20240102": 1.5},
            }
        }
    }


@patch("ingestors.nasa_power.httpx.get")
def test_fetch_creates_json(mock_get, nasa_ingestor):
    """fetch() downloads and saves JSON to bronze."""
    resp = MagicMock()
    resp.json.return_value = _mock_response()
    resp.raise_for_status = MagicMock()
    mock_get.return_value = resp

    paths = nasa_ingestor.fetch(start_date="2024-01-01", end_date="2024-01-02")
    assert len(paths) == 1
    assert paths[0].suffix == ".json"
    data = json.loads(paths[0].read_text())
    assert "T2M" in data["properties"]["parameter"]


@patch("ingestors.nasa_power.httpx.get")
def test_fetch_uses_aoi_coords(mock_get, nasa_ingestor):
    """fetch() passes AOI center coordinates to the API."""
    resp = MagicMock()
    resp.json.return_value = _mock_response()
    resp.raise_for_status = MagicMock()
    mock_get.return_value = resp

    nasa_ingestor.fetch(start_date="2024-01-01", end_date="2024-01-02")
    call_url = mock_get.call_args[1].get("url") or mock_get.call_args[0][0]
    assert "latitude=" in call_url
    assert "longitude=" in call_url

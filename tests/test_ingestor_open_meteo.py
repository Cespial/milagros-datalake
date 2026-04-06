"""Tests for Open-Meteo weather + flood ingestor."""

import json
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from ingestors.open_meteo import OpenMeteoIngestor


@pytest.fixture
def ingestor(tmp_catalog, tmp_path):
    bronze = tmp_path / "bronze"
    return OpenMeteoIngestor(catalog=tmp_catalog, bronze_root=bronze)


def _mock_weather_response():
    return {
        "latitude": 6.475,
        "longitude": -75.525,
        "daily": {
            "time": ["2024-01-01", "2024-01-02"],
            "temperature_2m_mean": [18.5, 19.1],
            "temperature_2m_max": [22.0, 23.1],
            "temperature_2m_min": [15.0, 16.2],
            "precipitation_sum": [2.3, 0.0],
            "rain_sum": [2.3, 0.0],
            "et0_fao_evapotranspiration": [3.1, 3.4],
            "windspeed_10m_max": [4.5, 3.2],
            "windgusts_10m_max": [9.1, 7.8],
            "shortwave_radiation_sum": [14.2, 16.5],
        },
    }


def _mock_flood_response():
    return {
        "latitude": 6.475,
        "longitude": -75.525,
        "daily": {
            "time": ["2024-01-01", "2024-01-02"],
            "river_discharge": [1.23, 1.45],
        },
    }


@patch("ingestors.open_meteo.httpx.get")
def test_fetch_creates_both_files(mock_get, ingestor):
    """fetch() downloads and saves both weather and flood JSON files."""
    responses = [MagicMock(), MagicMock()]
    responses[0].json.return_value = _mock_weather_response()
    responses[0].raise_for_status = MagicMock()
    responses[1].json.return_value = _mock_flood_response()
    responses[1].raise_for_status = MagicMock()
    mock_get.side_effect = responses

    paths = ingestor.fetch(start_date="2024-01-01", end_date="2024-01-02")

    assert len(paths) == 2
    assert all(p.suffix == ".json" for p in paths)
    filenames = {p.name for p in paths}
    assert "weather_daily.json" in filenames
    assert "flood_discharge.json" in filenames


@patch("ingestors.open_meteo.httpx.get")
def test_fetch_weather_content(mock_get, ingestor):
    """Weather JSON contains expected variables."""
    responses = [MagicMock(), MagicMock()]
    responses[0].json.return_value = _mock_weather_response()
    responses[0].raise_for_status = MagicMock()
    responses[1].json.return_value = _mock_flood_response()
    responses[1].raise_for_status = MagicMock()
    mock_get.side_effect = responses

    paths = ingestor.fetch(start_date="2024-01-01", end_date="2024-01-02")

    weather_path = next(p for p in paths if p.name == "weather_daily.json")
    data = json.loads(weather_path.read_text())
    daily = data["daily"]
    assert "temperature_2m_mean" in daily
    assert "precipitation_sum" in daily
    assert len(daily["time"]) == 2


@patch("ingestors.open_meteo.httpx.get")
def test_fetch_flood_content(mock_get, ingestor):
    """Flood JSON contains river_discharge variable."""
    responses = [MagicMock(), MagicMock()]
    responses[0].json.return_value = _mock_weather_response()
    responses[0].raise_for_status = MagicMock()
    responses[1].json.return_value = _mock_flood_response()
    responses[1].raise_for_status = MagicMock()
    mock_get.side_effect = responses

    paths = ingestor.fetch(start_date="2024-01-01", end_date="2024-01-02")

    flood_path = next(p for p in paths if p.name == "flood_discharge.json")
    data = json.loads(flood_path.read_text())
    assert "river_discharge" in data["daily"]
    assert data["daily"]["river_discharge"] == [1.23, 1.45]


@patch("ingestors.open_meteo.httpx.get")
def test_fetch_uses_aoi_center(mock_get, ingestor):
    """fetch() passes AOI centroid coordinates to both API calls."""
    responses = [MagicMock(), MagicMock()]
    responses[0].json.return_value = _mock_weather_response()
    responses[0].raise_for_status = MagicMock()
    responses[1].json.return_value = _mock_flood_response()
    responses[1].raise_for_status = MagicMock()
    mock_get.side_effect = responses

    ingestor.fetch(start_date="2024-01-01", end_date="2024-01-02")

    assert mock_get.call_count == 2
    for call in mock_get.call_args_list:
        params = call[1].get("params") or call[0][1]
        assert "latitude" in params
        assert "longitude" in params
        assert params["latitude"] == pytest.approx(6.475)
        assert params["longitude"] == pytest.approx(-75.525)


@patch("ingestors.open_meteo.httpx.get")
def test_fetch_skips_existing_files(mock_get, ingestor):
    """fetch() does not re-download files that already exist."""
    # Pre-create both files
    ingestor.bronze_dir.mkdir(parents=True, exist_ok=True)
    weather_path = ingestor.bronze_dir / "weather_daily.json"
    flood_path = ingestor.bronze_dir / "flood_discharge.json"
    weather_path.write_text(json.dumps(_mock_weather_response()))
    flood_path.write_text(json.dumps(_mock_flood_response()))

    paths = ingestor.fetch()

    mock_get.assert_not_called()
    assert len(paths) == 2


@patch("ingestors.open_meteo.httpx.get")
def test_fetch_weather_error_continues_to_flood(mock_get, ingestor):
    """If weather call fails, flood call still proceeds."""
    weather_resp = MagicMock()
    weather_resp.raise_for_status.side_effect = Exception("HTTP 500")
    flood_resp = MagicMock()
    flood_resp.json.return_value = _mock_flood_response()
    flood_resp.raise_for_status = MagicMock()
    mock_get.side_effect = [weather_resp, flood_resp]

    paths = ingestor.fetch(start_date="2024-01-01", end_date="2024-01-02")

    # Only flood file should exist
    assert len(paths) == 1
    assert paths[0].name == "flood_discharge.json"

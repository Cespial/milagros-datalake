"""Tests for GBIF / SiB Colombia biodiversity ingestor."""

import json
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from ingestors.gbif_sib import GbifSibIngestor


@pytest.fixture
def ingestor(tmp_catalog, tmp_path):
    bronze = tmp_path / "bronze"
    return GbifSibIngestor(catalog=tmp_catalog, bronze_root=bronze)


def _make_gbif_response(results, end_of_records=True):
    """Build a minimal GBIF occurrence search response."""
    return {
        "offset": 0,
        "limit": 300,
        "endOfRecords": end_of_records,
        "count": len(results),
        "results": results,
    }


def _sample_occurrence(species="Cattleya trianae", kingdom="Plantae"):
    return {
        "species": species,
        "scientificName": f"{species} Lindl.",
        "kingdom": kingdom,
        "phylum": "Tracheophyta",
        "class": "Liliopsida",
        "order": "Asparagales",
        "family": "Orchidaceae",
        "genus": species.split()[0],
        "decimalLatitude": 6.45,
        "decimalLongitude": -75.50,
        "year": 2022,
        "basisOfRecord": "HUMAN_OBSERVATION",
        "datasetName": "SiB Colombia",
        "country": "Colombia",
        "stateProvince": "Antioquia",
        "municipality": "San Pedro de los Milagros",
    }


@patch("ingestors.gbif_sib.httpx.get")
def test_fetch_creates_json_file(mock_get, ingestor):
    """fetch() saves occurrences to gbif_occurrences.json."""
    mock_resp = MagicMock()
    mock_resp.json.return_value = _make_gbif_response([_sample_occurrence()])
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    paths = ingestor.fetch()

    assert len(paths) == 1
    assert paths[0].name == "gbif_occurrences.json"
    assert paths[0].exists()


@patch("ingestors.gbif_sib.httpx.get")
def test_fetch_record_fields(mock_get, ingestor):
    """Each saved record contains all expected fields."""
    occ = _sample_occurrence()
    mock_resp = MagicMock()
    mock_resp.json.return_value = _make_gbif_response([occ])
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    paths = ingestor.fetch()
    records = json.loads(paths[0].read_text())

    assert len(records) == 1
    rec = records[0]
    assert rec["species"] == "Cattleya trianae"
    assert rec["kingdom"] == "Plantae"
    assert rec["lat"] == 6.45
    assert rec["lon"] == -75.50
    assert rec["year"] == 2022
    assert rec["basisOfRecord"] == "HUMAN_OBSERVATION"
    assert rec["stateProvince"] == "Antioquia"
    assert rec["municipality"] == "San Pedro de los Milagros"


@patch("ingestors.gbif_sib.httpx.get")
def test_fetch_paginates(mock_get, ingestor):
    """fetch() continues fetching pages until endOfRecords is True."""
    page1 = _make_gbif_response(
        [_sample_occurrence("Species alpha")], end_of_records=False
    )
    page2 = _make_gbif_response(
        [_sample_occurrence("Species beta")], end_of_records=True
    )

    responses = [MagicMock(), MagicMock()]
    responses[0].json.return_value = page1
    responses[0].raise_for_status = MagicMock()
    responses[1].json.return_value = page2
    responses[1].raise_for_status = MagicMock()
    mock_get.side_effect = responses

    paths = ingestor.fetch()
    records = json.loads(paths[0].read_text())

    assert mock_get.call_count == 2
    assert len(records) == 2
    species_names = {r["species"] for r in records}
    assert species_names == {"Species alpha", "Species beta"}


@patch("ingestors.gbif_sib.httpx.get")
def test_fetch_stops_on_empty_results(mock_get, ingestor):
    """fetch() stops when results list is empty."""
    mock_resp = MagicMock()
    mock_resp.json.return_value = _make_gbif_response([])
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    paths = ingestor.fetch()
    records = json.loads(paths[0].read_text())

    assert mock_get.call_count == 1
    assert records == []


@patch("ingestors.gbif_sib.httpx.get")
def test_fetch_skips_existing_file(mock_get, ingestor):
    """fetch() returns existing file without making API calls."""
    ingestor.bronze_dir.mkdir(parents=True, exist_ok=True)
    existing = ingestor.bronze_dir / "gbif_occurrences.json"
    existing.write_text(json.dumps([_sample_occurrence()]))

    paths = ingestor.fetch()

    mock_get.assert_not_called()
    assert len(paths) == 1
    assert paths[0] == existing


@patch("ingestors.gbif_sib.httpx.get")
def test_fetch_uses_aoi_bbox_params(mock_get, ingestor):
    """fetch() passes AOI bounding box to GBIF API."""
    mock_resp = MagicMock()
    mock_resp.json.return_value = _make_gbif_response([_sample_occurrence()])
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    ingestor.fetch()

    call_kwargs = mock_get.call_args
    params = call_kwargs[1].get("params") or call_kwargs[0][1]
    assert "6.25" in params["decimalLatitude"]
    assert "6.7" in params["decimalLatitude"]
    assert "-75.8" in params["decimalLongitude"]
    assert "-75.25" in params["decimalLongitude"]
    assert params["hasCoordinate"] == "true"
    assert params["hasGeospatialIssue"] == "false"


@patch("ingestors.gbif_sib.httpx.get")
def test_fetch_handles_missing_fields(mock_get, ingestor):
    """fetch() handles occurrences with missing optional fields gracefully."""
    sparse_occurrence = {"decimalLatitude": 6.40, "decimalLongitude": -75.55}
    mock_resp = MagicMock()
    mock_resp.json.return_value = _make_gbif_response([sparse_occurrence])
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    paths = ingestor.fetch()
    records = json.loads(paths[0].read_text())

    assert len(records) == 1
    rec = records[0]
    assert rec["species"] is None
    assert rec["kingdom"] is None
    assert rec["lat"] == 6.40
    assert rec["lon"] == -75.55


def test_ingestor_class_attributes(tmp_catalog, tmp_path):
    """GbifSibIngestor has the correct class-level metadata."""
    ingestor = GbifSibIngestor(catalog=tmp_catalog, bronze_root=tmp_path / "bronze")
    assert ingestor.name == "gbif_sib"
    assert ingestor.source_type == "api"
    assert ingestor.data_type == "tabular"
    assert ingestor.category == "biodiversidad"
    assert ingestor.schedule == "monthly"
    assert "CC" in ingestor.license

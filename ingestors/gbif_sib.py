"""GBIF / SiB Colombia ingestor — species occurrence records in AOI."""
import json
from pathlib import Path
import httpx
import structlog
from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

GBIF_API = "https://api.gbif.org/v1/occurrence/search"


class GbifSibIngestor(BaseIngestor):
    name = "gbif_sib"
    source_type = "api"
    data_type = "tabular"
    category = "biodiversidad"
    schedule = "monthly"
    license = "CC-BY-NC / CC0 (varies)"

    def fetch(self, **kwargs) -> list[Path]:
        out_path = self.bronze_dir / "gbif_occurrences.json"
        if out_path.exists():
            log.info("gbif_sib.skip_existing")
            return [out_path]

        # GBIF uses decimalLatitude/decimalLongitude with geometry WKT
        # Or simpler: bounding box filter
        all_records = []
        offset = 0
        limit = 300  # GBIF max per request

        while True:
            params = {
                "decimalLatitude": f"{AOI_BBOX['south']},{AOI_BBOX['north']}",
                "decimalLongitude": f"{AOI_BBOX['west']},{AOI_BBOX['east']}",
                "limit": limit,
                "offset": offset,
                "hasCoordinate": "true",
                "hasGeospatialIssue": "false",
            }
            log.info("gbif_sib.fetching", offset=offset)

            resp = httpx.get(GBIF_API, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()

            results = data.get("results", [])
            if not results:
                break

            # Extract key fields
            for r in results:
                all_records.append({
                    "species": r.get("species"),
                    "scientificName": r.get("scientificName"),
                    "kingdom": r.get("kingdom"),
                    "phylum": r.get("phylum"),
                    "class": r.get("class"),
                    "order": r.get("order"),
                    "family": r.get("family"),
                    "genus": r.get("genus"),
                    "lat": r.get("decimalLatitude"),
                    "lon": r.get("decimalLongitude"),
                    "year": r.get("year"),
                    "basisOfRecord": r.get("basisOfRecord"),
                    "datasetName": r.get("datasetName"),
                    "country": r.get("country"),
                    "stateProvince": r.get("stateProvince"),
                    "municipality": r.get("municipality"),
                })

            offset += limit
            end_of_records = data.get("endOfRecords", False)
            if end_of_records or offset >= 10000:  # Cap at 10K to avoid huge downloads
                break

        out_path.write_text(json.dumps(all_records, indent=2, ensure_ascii=False))
        log.info("gbif_sib.saved", records=len(all_records), path=str(out_path))
        return [out_path]

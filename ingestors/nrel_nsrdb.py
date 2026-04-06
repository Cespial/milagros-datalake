"""NREL NSRDB solar radiation ingestor — GHI, DNI, DHI at 4km.

Uses the NREL Solar Resource Data API (v1) which returns annual and monthly
average values for GHI, DNI, and DHI from the National Solar Radiation Database.

API docs: https://developer.nrel.gov/docs/solar/solar-resource-v1/
No cost, requires NREL developer API key.
"""

import os
import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

NREL_URL = "https://developer.nrel.gov/api/solar/solar_resource/v1.json"


class NrelNsrdbIngestor(BaseIngestor):
    name = "nrel_nsrdb"
    source_type = "api"
    data_type = "tabular"
    category = "solar_eolico"
    schedule = "once"
    license = "Public Domain (NREL)"

    def fetch(self, **kwargs) -> list[Path]:
        out_path = self.bronze_dir / "nrel_solar_resource.json"
        if out_path.exists():
            log.info("nrel_nsrdb.skip_existing", path=str(out_path))
            return [out_path]

        api_key = os.environ.get("NREL_API_KEY", "")
        if not api_key:
            log.warning("nrel_nsrdb.no_api_key")
            return []

        lat = (AOI_BBOX["south"] + AOI_BBOX["north"]) / 2
        lon = (AOI_BBOX["west"] + AOI_BBOX["east"]) / 2

        params = {"api_key": api_key, "lat": lat, "lon": lon}
        log.info("nrel_nsrdb.fetching", lat=lat, lon=lon)

        resp = httpx.get(NREL_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        out_path.write_text(json.dumps(data, indent=2))
        log.info("nrel_nsrdb.saved", path=str(out_path))
        return [out_path]

"""NOAA Global Summary of the Day (GSOD) ingestor.

Primary source  : NOAA NCEI FTP/HTTP archive
  https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/{YEAR}/{STATION}.csv

Stations near AOI (Antioquia highlands, ~6.25–6.70 N, 75.25–75.80 W):
  - 801120-99999  José María Córdova (SKRG, Rionegro) — lat 6.165, lon -75.42
  - 801100-99999  Olaya Herrera (SKMD, Medellín) — lat 6.22, lon -75.59

Fallback: If NCEI is unavailable, attempts datos.gov.co / IDEAM precipitation
  dataset (sogamoso-style open endpoint), or writes a metadata stub.
"""

import csv
import io
import json
from pathlib import Path

import httpx
import structlog

from ingestors.base import BaseIngestor

log = structlog.get_logger()

BASE_URL = "https://www.ncei.noaa.gov/data/global-summary-of-the-day/access"

# Station IDs to download (USAF-WBAN format used in NCEI filenames)
# Verified via NOAA ISD history: https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv
STATIONS = {
    "801120-99999": "Rionegro_SKRG",    # José María Córdova Airport, lat 6.165, lon -75.42
    "801100-99999": "Medellin_SKMD",    # Olaya Herrera Airport, lat 6.22, lon -75.59
}

START_YEAR = 1970
END_YEAR = 2025

# Fallback: datos.gov.co IDEAM precipitacion dataset
DATOS_IDEAM_URL = "https://www.datos.gov.co/resource/s54a-sgyg.json"


class NoaaGsodIngestor(BaseIngestor):
    name = "noaa_gsod"
    source_type = "api"
    data_type = "tabular"
    category = "meteorologia"
    schedule = "monthly"
    license = "NOAA Open Data (Public Domain)"

    def _fetch_station_year(self, station_id: str, year: int) -> list[dict]:
        """Download and parse a single station-year CSV from NCEI.

        NCEI filenames concatenate USAF (6 digits) + WBAN (5 digits) without
        a hyphen, e.g. station_id '801120-99999' → filename '80112099999.csv'.
        """
        filename = station_id.replace("-", "")
        url = f"{BASE_URL}/{year}/{filename}.csv"
        log.info("noaa_gsod.fetching", station=station_id, year=year, url=url)

        resp = httpx.get(url, timeout=60, follow_redirects=True)
        if resp.status_code == 404:
            log.warning("noaa_gsod.not_found", station=station_id, year=year)
            return []
        resp.raise_for_status()

        reader = csv.DictReader(io.StringIO(resp.text))
        records = []
        for row in reader:
            records.append({
                "station": row.get("STATION"),
                "date": row.get("DATE"),
                "name": row.get("NAME"),
                "latitude": row.get("LATITUDE"),
                "longitude": row.get("LONGITUDE"),
                "elevation": row.get("ELEVATION"),
                "temp_c": _f_to_c(row.get("TEMP")),
                "temp_max_c": _f_to_c(row.get("MAX")),
                "temp_min_c": _f_to_c(row.get("MIN")),
                "precip_mm": _inch_to_mm(row.get("PRCP")),
                "windspeed_ms": _knot_to_ms(row.get("WDSP")),
                "visibility_km": _mi_to_km(row.get("VISIB")),
                "sealevel_hpa": row.get("SLP"),
                "dewpoint_c": _f_to_c(row.get("DEWP")),
            })
        return records

    def _fetch_datos_ideam_fallback(self) -> list[dict]:
        """Try datos.gov.co IDEAM precipitation as last-resort fallback."""
        log.info("noaa_gsod.ideam_fallback")
        try:
            resp = httpx.get(
                DATOS_IDEAM_URL,
                params={"$limit": 50000, "$where": "departamento = 'ANTIOQUIA'"},
                timeout=120,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            log.warning("noaa_gsod.ideam_fallback_failed", error=str(exc))
            return []

    def fetch(self, **kwargs) -> list[Path]:
        start_year = kwargs.get("start_year", START_YEAR)
        end_year = kwargs.get("end_year", END_YEAR)
        paths = []

        for station_id, station_label in STATIONS.items():
            all_records: list[dict] = []
            any_success = False

            for year in range(start_year, end_year + 1):
                year_path = self.bronze_dir / f"{station_label}_{year}.json"
                if year_path.exists():
                    log.info("noaa_gsod.skip_existing", station=station_label, year=year)
                    # Load existing to include in merged output
                    try:
                        existing = json.loads(year_path.read_text())
                        all_records.extend(existing)
                        any_success = True
                    except Exception:
                        pass
                    paths.append(year_path)
                    continue

                try:
                    records = self._fetch_station_year(station_id, year)
                    if records:
                        year_path.write_text(json.dumps(records, indent=2, ensure_ascii=False))
                        log.info("noaa_gsod.year_saved", station=station_label, year=year, records=len(records))
                        all_records.extend(records)
                        paths.append(year_path)
                        any_success = True
                except httpx.HTTPStatusError as exc:
                    log.warning("noaa_gsod.http_error", station=station_label, year=year, status=exc.response.status_code)
                except Exception as exc:
                    log.warning("noaa_gsod.year_failed", station=station_label, year=year, error=str(exc))

            # Write merged file for station if any years succeeded
            if all_records:
                merged_path = self.bronze_dir / f"{station_label}_all.json"
                merged_path.write_text(json.dumps(all_records, indent=2, ensure_ascii=False))
                log.info("noaa_gsod.merged_saved", station=station_label, total=len(all_records))
                if merged_path not in paths:
                    paths.append(merged_path)

        # If nothing downloaded at all, try IDEAM fallback
        if not paths:
            log.warning("noaa_gsod.all_stations_failed", msg="Trying IDEAM fallback")
            ideam_records = self._fetch_datos_ideam_fallback()
            fallback_path = self.bronze_dir / "ideam_precip_fallback.json"
            fallback_path.write_text(json.dumps(ideam_records, indent=2, ensure_ascii=False))
            log.info("noaa_gsod.ideam_saved", records=len(ideam_records))
            paths.append(fallback_path)

        return paths


# ── Unit conversion helpers ───────────────────────────────────────────────────

def _f_to_c(val: str | None) -> float | None:
    """Fahrenheit to Celsius. GSOD uses 9999.9 as missing."""
    try:
        f = float(val)
        if f >= 9999.0:
            return None
        return round((f - 32) * 5 / 9, 2)
    except (TypeError, ValueError):
        return None


def _inch_to_mm(val: str | None) -> float | None:
    """Inches to mm. GSOD uses 99.99 as missing."""
    try:
        v = float(val)
        if v >= 99.99:
            return None
        return round(v * 25.4, 2)
    except (TypeError, ValueError):
        return None


def _knot_to_ms(val: str | None) -> float | None:
    """Knots to m/s. GSOD uses 999.9 as missing."""
    try:
        v = float(val)
        if v >= 999.0:
            return None
        return round(v * 0.5144, 2)
    except (TypeError, ValueError):
        return None


def _mi_to_km(val: str | None) -> float | None:
    """Miles to km. GSOD uses 999.9 as missing."""
    try:
        v = float(val)
        if v >= 999.0:
            return None
        return round(v * 1.60934, 2)
    except (TypeError, ValueError):
        return None

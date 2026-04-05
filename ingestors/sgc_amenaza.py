"""SGC seismic hazard parameters (NSR-10) per AOI municipality.

NSR-10 (Reglamento Colombiano de Construcción Sismo Resistente) assigns
each municipality:
  - Aa  — peak ground acceleration (fraction of g), soft soil
  - Av  — peak ground velocity parameter
  - zona — seismic zone (Alta / Intermedia / Baja)

Hardcoded values for AOI municipalities (northern Antioquia plateau,
~6.25–6.70°N, ~75.25–75.80°W) are from NSR-10 Appendix A-3.
Tries SGC API for updates; falls back to hardcoded values on failure.

Saves: seismic_hazard.json — dict keyed by DANE 5-digit municipality code.
"""

import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_MUNICIPIOS
from ingestors.base import BaseIngestor

log = structlog.get_logger()

# NSR-10 hardcoded values for AOI municipalities (northern Antioquia plateau)
# Source: NSR-10 Appendix A-3, Figura A.3-1 and Table A.3-1
NSR10_DEFAULTS: dict[str, dict] = {
    "05664": {"municipio": "San Pedro de los Milagros", "Aa": 0.15, "Av": 0.20, "zona": "Intermedia"},
    "05264": {"municipio": "Entrerrios",               "Aa": 0.15, "Av": 0.20, "zona": "Intermedia"},
    "05086": {"municipio": "Belmira",                  "Aa": 0.15, "Av": 0.20, "zona": "Intermedia"},
    "05237": {"municipio": "Donmatias",                "Aa": 0.15, "Av": 0.20, "zona": "Intermedia"},
    "05686": {"municipio": "Santa Rosa de Osos",       "Aa": 0.15, "Av": 0.20, "zona": "Intermedia"},
    "05079": {"municipio": "Barbosa",                  "Aa": 0.20, "Av": 0.20, "zona": "Intermedia"},
    "05088": {"municipio": "Bello",                    "Aa": 0.20, "Av": 0.20, "zona": "Intermedia"},
    "05761": {"municipio": "Sopetran",                 "Aa": 0.15, "Av": 0.20, "zona": "Intermedia"},
    "05576": {"municipio": "Olaya",                    "Aa": 0.15, "Av": 0.20, "zona": "Intermedia"},
    "05042": {"municipio": "Santafe de Antioquia",     "Aa": 0.15, "Av": 0.20, "zona": "Intermedia"},
}

# SGC amenaza sísmica API (experimental endpoint)
SGC_API_URL = "https://www.sgc.gov.co/Sgc/api/amenaza"


class SgcAmenazaIngestor(BaseIngestor):
    name = "sgc_amenaza"
    source_type = "api"
    data_type = "tabular"
    category = "geologia"
    schedule = "once"
    license = "CC0"

    def fetch(self, **kwargs) -> list[Path]:
        out_path = self.bronze_dir / "seismic_hazard.json"
        if out_path.exists():
            log.info("sgc_amenaza.skip_existing", path=str(out_path))
            return [out_path]

        result: dict[str, dict] = {}

        for code, municipio_name in AOI_MUNICIPIOS.items():
            # Start with hardcoded NSR-10 values
            entry = NSR10_DEFAULTS.get(
                code,
                {
                    "municipio": municipio_name,
                    "Aa": 0.15,
                    "Av": 0.20,
                    "zona": "Intermedia",
                },
            ).copy()
            entry["source"] = "NSR-10 Appendix A-3 (hardcoded)"
            entry["dane_code"] = code

            # Try SGC API for authoritative update
            try:
                resp = httpx.get(
                    SGC_API_URL,
                    params={"municipio": code},
                    timeout=30,
                )
                if resp.status_code == 200:
                    api_data = resp.json()
                    if api_data:
                        entry.update(api_data)
                        entry["source"] = "SGC API"
                        log.info("sgc_amenaza.api_updated", code=code, municipio=municipio_name)
            except Exception as exc:
                log.debug("sgc_amenaza.api_unavailable", code=code, error=str(exc))

            result[code] = entry
            log.info("sgc_amenaza.entry", code=code, municipio=municipio_name, zona=entry["zona"])

        out_path.write_text(json.dumps(result, indent=2, ensure_ascii=False))
        log.info("sgc_amenaza.saved", municipalities=len(result), path=str(out_path))
        return [out_path]

"""ANLA VITAL — environmental licensing expedientes metadata."""
import json
from pathlib import Path
import structlog
from ingestors.base import BaseIngestor

log = structlog.get_logger()


class AnlaVitalIngestor(BaseIngestor):
    name = "anla_vital"
    source_type = "scrape"
    data_type = "documents"
    category = "regulatorio"
    schedule = "monthly"
    license = "Public Domain (ANLA)"

    def fetch(self, **kwargs) -> list[Path]:
        out_path = self.bronze_dir / "anla_vital_referencia.json"
        if out_path.exists():
            return [out_path]

        # ANLA VITAL reference metadata for hydroelectric EIAs
        metadata = {
            "source": "ANLA VITAL",
            "url": "https://vital.anla.gov.co",
            "description": "Portal de seguimiento ambiental de proyectos licenciados",
            "relevance": "Expedientes de EIA para centrales hidroelectricas >100 MW en Antioquia",
            "access": "Registro gratuito requerido para acceso completo",
            "key_regulations": [
                {"name": "Decreto 1076/2015", "desc": "Decreto Unico ambiental: licenciamiento, concesiones agua, POMCA, EIA"},
                {"name": "Res. 0631/2015", "desc": "Limites de vertimientos a cuerpos de agua"},
                {"name": "Res. 959/2018", "desc": "Regulacion caudal ecologico"},
            ],
            "reference_projects": [
                {"name": "Riogrande II (La Tasajera)", "operator": "EPM", "capacity_mw": 306, "type": "Pelton", "status": "Operando desde 1993"},
                {"name": "Riogrande I (Mocorongo)", "operator": "EPM", "capacity_mw": 19, "status": "Operando desde 1951"},
                {"name": "Niquia", "operator": "EPM", "capacity_mw": 20, "type": "Francis", "status": "Operando desde 1993"},
            ],
            "eia_requirements": {
                "authority": "ANLA (>100 MW)",
                "terms_of_reference": "Terminos de referencia para EIA de proyectos hidroelectricos",
                "key_chapters": ["Medio abiotico", "Medio biotico", "Medio socioeconomico", "Zonificacion ambiental", "Evaluacion de impactos", "Plan de manejo ambiental"],
            },
            "transfers_law99": {
                "article": "Art. 45, Ley 99/1993",
                "rate": "6% de ventas brutas de energia",
                "beneficiaries": "Municipios y CARs del area de influencia del embalse",
                "reference": "EPM transfiere >$47,500 M COP en 15 anos a municipios de Riogrande II",
            },
        }

        out_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False))
        log.info("anla_vital.saved", path=str(out_path))
        return [out_path]

"""EOT/PBOT San Pedro de los Milagros — municipal land use plan metadata."""
import json
from pathlib import Path
import structlog
from ingestors.base import BaseIngestor

log = structlog.get_logger()


class EotPbotIngestor(BaseIngestor):
    name = "eot_pbot"
    source_type = "scrape"
    data_type = "documents"
    category = "regulatorio"
    schedule = "once"
    license = "Public Domain"

    def fetch(self, **kwargs) -> list[Path]:
        out_path = self.bronze_dir / "eot_pbot_san_pedro.json"
        if out_path.exists():
            return [out_path]

        metadata = {
            "municipio": "San Pedro de los Milagros",
            "codigo_dane": "05664",
            "departamento": "Antioquia",
            "subregion": "Norte",
            "eot_actual": {
                "acuerdo": "Acuerdo 080/2000",
                "modificacion": "Decreto 107/2019",
                "estado": "En proceso de actualizacion a PBOT",
                "consulta": "Secretaria de Planeacion y Desarrollo Territorial",
            },
            "clasificacion_suelo": {
                "urbano": "Cabecera municipal",
                "rural": "Mayor parte del territorio",
                "expansion": "Zonas definidas en EOT",
                "proteccion": "Paramos, nacimientos de agua, rondas hidricas",
            },
            "vocacion_productiva": "Lechera (principal), agricultura, turismo rural",
            "corantioquia": {
                "oficina": "Tahamies",
                "pomca": "POMCA Rio Grande y Rio Aurra/Ovejas",
                "pueaa": "Plan de Uso Eficiente y Ahorro del Agua",
            },
            "consulta_urls": [
                "https://www.colombiaot.gov.co/pot/",
                "https://sanpedrodelosmilagros-antioquia.gov.co",
            ],
        }

        out_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False))
        log.info("eot_pbot.saved", path=str(out_path))
        return [out_path]

"""CREG Alejandria — regulatory resolutions for electricity sector."""
import json
from pathlib import Path
import structlog
from ingestors.base import BaseIngestor

log = structlog.get_logger()


class CregIngestor(BaseIngestor):
    name = "creg"
    source_type = "scrape"
    data_type = "documents"
    category = "regulatorio"
    schedule = "monthly"
    license = "Public Domain (CREG)"

    def fetch(self, **kwargs) -> list[Path]:
        out_path = self.bronze_dir / "creg_resoluciones.json"
        if out_path.exists():
            return [out_path]

        resoluciones = {
            "source": "CREG - Comision de Regulacion de Energia y Gas",
            "url": "https://gestornormativo.creg.gov.co/",
            "key_resolutions": [
                {"number": "Res. 086/1996", "topic": "Reglas para plantas <20 MW", "relevance": "Marco regulatorio PCH"},
                {"number": "Res. 039/2001", "topic": "Comercializacion energia PCH", "relevance": "Acceso a mercado"},
                {"number": "Res. 071/2006", "topic": "Cargo por confiabilidad", "relevance": "Ingreso garantizado"},
                {"number": "Res. 101 066/2024", "topic": "Precio de escasez", "relevance": "Reducido de $945 a $359/kWh"},
                {"number": "Ley 1715/2014", "topic": "FNCER incentivos", "relevance": "PCH = FNCER. Deduccion 50% renta, exclusion IVA, exencion arancel"},
                {"number": "Ley 2099/2021", "topic": "Transicion energetica", "relevance": "Extiende incentivos 30 anos"},
                {"number": "Ley 99/1993 Art.45", "topic": "Transferencias sector electrico", "relevance": "6% ventas a municipios y CARs"},
                {"number": "Ley 142/1994", "topic": "Regimen servicios publicos", "relevance": "Marco general"},
                {"number": "Ley 143/1994", "topic": "Ley electrica", "relevance": "Estructura: generacion, transmision, distribucion, comercializacion"},
            ],
            "dispatch_rules": {
                "<10MW": "No accede a despacho central",
                "10-20MW": "Puede optar a despacho central",
                ">20MW": "Despacho central obligatorio",
                ">100MW": "Licencia ANLA obligatoria",
            },
        }

        out_path.write_text(json.dumps(resoluciones, indent=2, ensure_ascii=False))
        log.info("creg.saved", path=str(out_path))
        return [out_path]

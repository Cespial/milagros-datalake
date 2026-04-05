"""IGAC cartography ingestor via WFS.

Source: geoportal.igac.gov.co
Layers (1:100,000):
  - Mpios100K        — municipalities
  - Drenaje_Sencillo100K  — simple drainage (rivers)
  - Curva_Nivel100K  — contour lines
  - Via100K          — roads
  - Cuerpo_Agua100K  — water bodies

Outputs one GeoJSON per layer, clipped to AOI_BBOX.
"""

import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

WFS_URL = "https://geoportal.igac.gov.co/geoserver/igac/wfs"

LAYERS = [
    "igac:Mpios100K",
    "igac:Drenaje_Sencillo100K",
    "igac:Curva_Nivel100K",
    "igac:Via100K",
    "igac:Cuerpo_Agua100K",
]

MAX_FEATURES = 50000


class IgacCartografiaIngestor(BaseIngestor):
    name = "igac_cartografia"
    source_type = "wfs"
    data_type = "vector"
    category = "geoespacial"
    schedule = "once"
    license = "IGAC Datos Abiertos"

    def _layer_filename(self, layer: str) -> str:
        """Convert 'igac:Mpios100K' → 'mpios100k.geojson'."""
        return layer.split(":")[-1].lower() + ".geojson"

    def fetch(self, **kwargs) -> list[Path]:
        bbox_str = (
            f"{AOI_BBOX['west']},{AOI_BBOX['south']}"
            f",{AOI_BBOX['east']},{AOI_BBOX['north']}"
        )
        paths = []

        for layer in LAYERS:
            filename = self._layer_filename(layer)
            out_path = self.bronze_dir / filename

            if out_path.exists():
                log.info("igac_cartografia.skip_existing", layer=layer)
                paths.append(out_path)
                continue

            log.info("igac_cartografia.fetching", layer=layer)

            params = {
                "service": "WFS",
                "version": "2.0.0",
                "request": "GetFeature",
                "typeName": layer,
                "outputFormat": "application/json",
                "srsName": "EPSG:4326",
                "bbox": f"{bbox_str},EPSG:4326",
                "count": MAX_FEATURES,
            }

            response = httpx.get(WFS_URL, params=params, timeout=300)
            response.raise_for_status()

            # Server may return JSON or raise — validate
            data = response.json()
            features = data.get("features", [])

            out_path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
            log.info(
                "igac_cartografia.saved",
                layer=layer,
                path=str(out_path),
                features=len(features),
            )
            paths.append(out_path)

        return paths

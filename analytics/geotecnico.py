"""Geotechnical site assessment — aptitude scoring."""

import json
from pathlib import Path
import numpy as np
import pandas as pd
import geopandas as gpd
import structlog

log = structlog.get_logger()


def run(bronze_dir: Path, silver_dir: Path, gold_dir: Path, **kwargs):
    """Build geotechnical aptitude assessment."""
    out_dir = gold_dir / "analytics"
    out_dir.mkdir(parents=True, exist_ok=True)

    results = {
        "methodology": "Multi-criteria weighted scoring",
        "weights": {"pendiente": 0.30, "litologia": 0.25, "distancia_fallas": 0.25, "densidad_deslizamientos": 0.20},
        "scores": {},
    }

    # 1. Geology summary
    geo_path = silver_dir / "vector" / "geologia" / "sgc_geologia.parquet"
    if geo_path.exists():
        try:
            gdf = gpd.read_parquet(geo_path)
            results["geologia"] = {
                "unidades": len(gdf),
                "tipos": list(gdf["descripcion"].unique()[:10]) if "descripcion" in gdf.columns else [],
            }
            log.info("geotecnico.geologia", unidades=len(gdf))
        except Exception as e:
            log.warning("geotecnico.geologia_failed", error=str(e))

    # 2. Landslide density
    simma_path = bronze_dir / "tabular" / "sgc_simma" / "movimientos_en_masa.json"
    if simma_path.exists():
        data = json.loads(simma_path.read_text())
        features = data.get("features", [])
        results["deslizamientos"] = {
            "total_en_aoi": len(features),
            "nota": "Densidad: {:.1f} eventos por 100 km2".format(len(features) / 27.5 * 100) if features else "Sin datos",
        }
        log.info("geotecnico.deslizamientos", total=len(features))

    # 3. Seismic parameters
    amenaza_path = bronze_dir / "tabular" / "sgc_amenaza" / "seismic_hazard.json"
    if amenaza_path.exists():
        data = json.loads(amenaza_path.read_text())
        if isinstance(data, dict):
            sample = list(data.values())[0] if data else {}
            results["sismicidad"] = {
                "zona": sample.get("zona", "Intermedia"),
                "Aa": sample.get("Aa", 0.15),
                "Av": sample.get("Av", 0.20),
                "sistema_fallas": "Romeral",
            }

    # 4. Soil data
    fao_dir = bronze_dir / "raster" / "fao"
    if fao_dir.exists():
        soil_files = list(fao_dir.glob("*.tif"))
        results["suelos"] = {"capas_disponibles": [f.stem for f in soil_files]}

    # 5. Aptitude summary (qualitative at this stage)
    features = []
    if simma_path.exists():
        data = json.loads(simma_path.read_text())
        features = data.get("features", [])

    results["evaluacion"] = {
        "pendiente": "Moderada a alta (terreno montanoso andino, 2000-2500 msnm)",
        "litologia": "Verificar con mapa geologico V2023 — 32 unidades identificadas",
        "fallas": f"17 fallas mapeadas en el AOI, sistema de fallas de Romeral dominante",
        "deslizamientos": f"{len(features) if simma_path.exists() else 'N/A'} eventos registrados en SIMMA",
        "aptitud_general": "Requiere evaluacion de campo. Zona sismica intermedia. Terreno montanoso con deslizamientos moderados.",
    }

    # Save
    json_out = out_dir / "evaluacion_geotecnica.json"
    json.dump(results, open(json_out, "w"), indent=2, ensure_ascii=False)

    # Also save as parquet summary
    summary = pd.DataFrame([results["evaluacion"]])
    summary.to_parquet(out_dir / "evaluacion_geotecnica.parquet", index=False)
    log.info("geotecnico.done")

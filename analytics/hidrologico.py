"""Hydrological model — 3 sources of discharge data."""

import json
from pathlib import Path
import numpy as np
import pandas as pd
import structlog

log = structlog.get_logger()


def run(bronze_dir: Path, silver_dir: Path, gold_dir: Path, **kwargs):
    """Build hydrological analysis from 3 sources."""
    out_dir = gold_dir / "analytics"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "modelo_hidrologico.parquet"

    results = {}

    # Source A: GloFAS via Open-Meteo (already in Gold)
    flood_path = bronze_dir / "tabular" / "open_meteo" / "flood_discharge.json"
    if flood_path.exists():
        data = json.loads(flood_path.read_text())
        daily = data.get("daily", {})
        dates = daily.get("time", [])
        discharge = daily.get("river_discharge", [])
        df_glofas = pd.DataFrame({"fecha": dates, "caudal_glofas_m3s": discharge})
        df_glofas["fecha"] = pd.to_datetime(df_glofas["fecha"], errors="coerce")
        df_glofas["caudal_glofas_m3s"] = pd.to_numeric(df_glofas["caudal_glofas_m3s"], errors="coerce")
        df_glofas = df_glofas.dropna()

        q = df_glofas["caudal_glofas_m3s"].values
        results["glofas"] = {
            "Q05": round(float(np.percentile(q, 95)), 3),
            "Q50": round(float(np.percentile(q, 50)), 3),
            "Q75": round(float(np.percentile(q, 25)), 3),
            "Q95": round(float(np.percentile(q, 5)), 3),
            "mean": round(float(np.mean(q)), 3),
            "records": len(q),
            "start": str(df_glofas["fecha"].min().date()),
            "end": str(df_glofas["fecha"].max().date()),
            "confianza": "media",
            "fuente": "Open-Meteo/GloFAS",
        }
        log.info("hidrologico.glofas", Q95=results["glofas"]["Q95"], records=len(q))
    else:
        df_glofas = pd.DataFrame()

    # Source B: IDEAM nivel (Silver) — statistics only, no conversion to Q
    nivel_stats = []
    for nivel_var in ["ideam_nivel_instantaneo", "ideam_nivel_maximo", "ideam_nivel_minimo"]:
        nivel_dir = silver_dir / "tabular" / "hidrologia" / nivel_var
        if not nivel_dir.exists():
            continue
        frames = []
        for p in sorted(nivel_dir.rglob("*.parquet")):
            try:
                df = pd.read_parquet(p)
                if not df.empty:
                    frames.append(df)
            except:
                continue
        if frames:
            combined = pd.concat(frames, ignore_index=True)
            val_col = next((c for c in combined.columns if "valor" in c), None)
            if val_col:
                vals = pd.to_numeric(combined[val_col], errors="coerce").dropna()
                stat = {
                    "variable": nivel_var.replace("ideam_", ""),
                    "P05": round(float(np.percentile(vals, 5)), 1),
                    "P25": round(float(np.percentile(vals, 25)), 1),
                    "P50": round(float(np.percentile(vals, 50)), 1),
                    "P75": round(float(np.percentile(vals, 75)), 1),
                    "P95": round(float(np.percentile(vals, 95)), 1),
                    "mean": round(float(np.mean(vals)), 1),
                    "records": len(vals),
                    "unit": "cm",
                    "confianza": "alta (nivel observado, requiere curva de calibracion para caudal)",
                    "fuente": "IDEAM DHIME",
                }
                nivel_stats.append(stat)
                log.info("hidrologico.ideam_nivel", variable=nivel_var, P50=stat["P50"], records=len(vals))
    results["ideam_nivel"] = nivel_stats

    # Source C: Regionalization — transpose GloFAS Q using drainage area ratio
    # Q_site = Q_glofas * (A_site / A_glofas)^0.8
    # GloFAS grid cell is ~25 km2, typical subcatchment ~50-200 km2
    area_glofas_km2 = 25.0  # approximate GloFAS cell area
    area_site_km2_options = [50, 100, 150, 200, 300, 500]  # sweep of possible catchment areas

    if "glofas" in results:
        regional = []
        for area in area_site_km2_options:
            ratio = (area / area_glofas_km2) ** 0.8
            regional.append({
                "area_cuenca_km2": area,
                "Q95_regionalizado": round(results["glofas"]["Q95"] * ratio, 3),
                "Q50_regionalizado": round(results["glofas"]["Q50"] * ratio, 3),
                "Q05_regionalizado": round(results["glofas"]["Q05"] * ratio, 3),
                "mean_regionalizado": round(results["glofas"]["mean"] * ratio, 3),
                "factor_transposicion": round(ratio, 3),
            })
        results["regionalizacion"] = regional
        log.info("hidrologico.regionalizacion", areas=len(regional))

    # Summary table
    summary = pd.DataFrame([
        {"fuente": "GloFAS (Open-Meteo)", "Q95_m3s": results.get("glofas", {}).get("Q95"), "Q50_m3s": results.get("glofas", {}).get("Q50"), "confianza": "media"},
    ])

    if results.get("regionalizacion"):
        for r in results["regionalizacion"]:
            summary = pd.concat([summary, pd.DataFrame([{
                "fuente": f"Regionalizado (A={r['area_cuenca_km2']}km2)",
                "Q95_m3s": r["Q95_regionalizado"],
                "Q50_m3s": r["Q50_regionalizado"],
                "confianza": "baja-media",
            }])], ignore_index=True)

    summary.to_parquet(out_path, index=False)

    # Export JSON for dashboard
    json_out = gold_dir / "analytics" / "modelo_hidrologico.json"
    json.dump(results, open(json_out, "w"), indent=2, ensure_ascii=False, default=str)
    log.info("hidrologico.done", output=str(out_path))

"""Gold view: Hydropower generation potential.

Formula: P = Q * H * eta * rho * g
Where:
  Q = discharge (m3/s) from flow duration curves
  H = hydraulic head (m), swept from 50 to 1000 m
  eta = turbine efficiency (Pelton=0.85, Francis=0.90)
  rho = 1000 kg/m3 (water density)
  g = 9.81 m/s2

Reads from gold/curvas_duracion.parquet.
Output: gold/potencial_generacion.parquet.
"""

from pathlib import Path

import pandas as pd
import structlog

from catalog.manager import CatalogManager

log = structlog.get_logger()

RHO = 1000.0   # kg/m3
G = 9.81       # m/s2

HEADS_M = [50, 100, 150, 200, 300, 400, 500, 600, 700, 800, 900, 1000]

TURBINES = [
    {"name": "Pelton", "eta": 0.85},
    {"name": "Francis", "eta": 0.90},
]

OUT_FILE = "potencial_generacion.parquet"


def build(bronze_dir: Path, silver_dir: Path, gold_dir: Path, catalog: CatalogManager, **kwargs):
    """Build hydropower potential matrix from flow duration curves."""
    out_path = gold_dir / OUT_FILE
    gold_dir.mkdir(parents=True, exist_ok=True)

    fdc_path = gold_dir / "curvas_duracion.parquet"
    if not fdc_path.exists():
        log.warning("potencial_generacion.no_fdc", path=str(fdc_path))
        pd.DataFrame().to_parquet(out_path, index=False)
        catalog.register({
            "dataset_id": "potencial_generacion",
            "source": "IDEAM DHIME / Ingenieria",
            "category": "hidrologia",
            "data_type": "tabular",
            "layer": "gold",
            "file_path": str(out_path),
            "format": "parquet",
            "ingestor": "processor.gold.potencial_generacion",
            "status": "empty",
            "notes": "Depends on curvas_duracion.parquet — not yet built",
        })
        return

    df_fdc = pd.read_parquet(fdc_path)

    if df_fdc.empty or "caudal_m3s" not in df_fdc.columns or "exceedance_pct" not in df_fdc.columns:
        log.warning("potencial_generacion.empty_fdc")
        pd.DataFrame(
            columns=["exceedance_pct", "head_m", "turbine", "eta", "q_m3s", "power_kw", "period"]
        ).to_parquet(out_path, index=False)
        catalog.register({
            "dataset_id": "potencial_generacion",
            "source": "IDEAM DHIME / Ingenieria",
            "category": "hidrologia",
            "data_type": "tabular",
            "layer": "gold",
            "file_path": str(out_path),
            "format": "parquet",
            "ingestor": "processor.gold.potencial_generacion",
            "status": "empty",
            "notes": "FDC missing required columns",
        })
        return

    rows = []

    # Use annual FDC for the sweep; include all periods if desired
    for _, fdc_row in df_fdc.iterrows():
        q = float(fdc_row["caudal_m3s"]) if pd.notna(fdc_row["caudal_m3s"]) else None
        if q is None or q <= 0:
            continue

        exceedance = fdc_row.get("exceedance_pct")
        period = fdc_row.get("period", "anual")

        for head in HEADS_M:
            for turbine in TURBINES:
                # Power in Watts = rho * g * Q * H * eta
                power_w = RHO * G * q * head * turbine["eta"]
                power_kw = power_w / 1000.0

                rows.append({
                    "exceedance_pct": exceedance,
                    "period": period,
                    "head_m": head,
                    "turbine": turbine["name"],
                    "eta": turbine["eta"],
                    "q_m3s": q,
                    "power_kw": round(power_kw, 2),
                    "energy_annual_gwh": round(power_kw * 8760 / 1e6, 4),  # assume full-year operation
                })

    if not rows:
        result = pd.DataFrame(
            columns=["exceedance_pct", "period", "head_m", "turbine", "eta", "q_m3s", "power_kw", "energy_annual_gwh"]
        )
    else:
        result = pd.DataFrame(rows)

    log.info("potencial_generacion.done", rows=len(result))
    result.to_parquet(out_path, index=False)

    catalog.register({
        "dataset_id": "potencial_generacion",
        "source": "IDEAM DHIME / Ingenieria",
        "category": "hidrologia",
        "data_type": "tabular",
        "layer": "gold",
        "file_path": str(out_path),
        "format": "parquet",
        "ingestor": "processor.gold.potencial_generacion",
        "status": "complete",
        "notes": (
            f"Hydropower potential: heads {HEADS_M[0]}-{HEADS_M[-1]} m, "
            f"turbines Pelton(eta=0.85) & Francis(eta=0.90), "
            f"rho={RHO}, g={G}"
        ),
    })

"""Basic financial model — LCOE, factor de planta, ingreso anual."""

import json
from pathlib import Path
import pandas as pd
import structlog

log = structlog.get_logger()

# Benchmarks for hydro >100 MW
CAPEX_USD_PER_KW = {"low": 1500, "mid": 2000, "high": 2500}
OPEX_PCT_CAPEX = 0.02
DISCOUNT_RATE = 0.10
PROJECT_LIFE_YEARS = 30
COP_PER_USD = 4200


def run(bronze_dir: Path, silver_dir: Path, gold_dir: Path, **kwargs):
    """Generate basic financial indicators."""
    out_dir = gold_dir / "analytics"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Read potencial_generacion Gold view
    potencial_path = gold_dir / "potencial_generacion.parquet"
    if not potencial_path.exists():
        log.warning("financiero.no_potencial")
        return

    df = pd.read_parquet(potencial_path)
    if df.empty:
        log.warning("financiero.empty_potencial")
        return

    # Read average spot price from XM SiMEM
    avg_price_cop_kwh = 350  # fallback average
    market_path = gold_dir / "mercado_despacho.parquet"
    if market_path.exists():
        try:
            mdf = pd.read_parquet(market_path)
            # Try to extract average price
            for col in mdf.columns:
                if "precio" in col.lower() or "price" in col.lower():
                    vals = pd.to_numeric(mdf[col], errors="coerce").dropna()
                    if len(vals) > 0:
                        avg_price_cop_kwh = round(vals.mean(), 0)
                        break
        except Exception:
            pass

    avg_price_usd_mwh = avg_price_cop_kwh / COP_PER_USD * 1000

    results = []
    # Filter to Q50 and Q95 with key head values
    if "period" in df.columns:
        key_cases = df[
            (df["exceedance_pct"].isin([50, 75, 95])) &
            (df["period"] == "anual")
        ].copy()
    else:
        key_cases = df[df["exceedance_pct"].isin([50, 75, 95])].copy()

    head_col = next((c for c in df.columns if "head" in c), None)
    power_col = next((c for c in df.columns if "power" in c.lower()), None)
    energy_col = next((c for c in df.columns if "energy" in c.lower() or "gwh" in c.lower()), None)

    if not all([head_col, power_col, energy_col]):
        log.warning("financiero.missing_columns", cols=list(df.columns))
        return

    for _, row in key_cases.iterrows():
        power_kw = row[power_col]
        power_mw = power_kw / 1000 if power_kw > 100 else power_kw  # handle kW vs MW
        energy_gwh = row[energy_col]
        head_m = row[head_col]

        if power_mw <= 0:
            continue

        factor_planta = energy_gwh * 1000 / (power_mw * 8760) if power_mw > 0 else 0

        for scenario, capex_per_kw in CAPEX_USD_PER_KW.items():
            capex_musd = power_mw * 1000 * capex_per_kw / 1e6
            opex_musd = capex_musd * OPEX_PCT_CAPEX
            ingreso_musd = energy_gwh * 1000 * avg_price_usd_mwh / 1e6
            lcoe = (capex_musd / PROJECT_LIFE_YEARS + opex_musd) / (energy_gwh + 0.001) * 1000  # USD/MWh

            results.append({
                "exceedance_pct": int(row.get("exceedance_pct", 0)),
                "head_m": int(head_m),
                "power_mw": round(power_mw, 1),
                "energy_gwh": round(energy_gwh, 1),
                "factor_planta": round(factor_planta, 3),
                "capex_scenario": scenario,
                "capex_usd_kw": capex_per_kw,
                "capex_musd": round(capex_musd, 1),
                "opex_musd_yr": round(opex_musd, 2),
                "lcoe_usd_mwh": round(lcoe, 1),
                "ingreso_anual_musd": round(ingreso_musd, 2),
                "precio_bolsa_cop_kwh": avg_price_cop_kwh,
            })

    out_df = pd.DataFrame(results)
    out_df.to_parquet(out_dir / "modelo_financiero.parquet", index=False)
    json.dump(results, open(out_dir / "modelo_financiero.json", "w"), indent=2)
    log.info("financiero.done", cases=len(results))

"""Solar complementarity — hydro + floating solar assessment."""

import json
from pathlib import Path
import pandas as pd
import structlog

log = structlog.get_logger()


def run(bronze_dir: Path, silver_dir: Path, gold_dir: Path, **kwargs):
    """Assess solar complementarity with hydroelectric generation."""
    out_dir = gold_dir / "analytics"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Read NASA POWER daily data for monthly solar/hydro comparison
    power_path = bronze_dir / "tabular" / "nasa_power" / "nasa_power_19810101_20260401.json"
    flood_path = bronze_dir / "tabular" / "open_meteo" / "flood_discharge.json"

    results = {"monthly_comparison": [], "summary": {}}

    # Monthly solar radiation
    avg_ghi = {}
    if power_path.exists():
        data = json.loads(power_path.read_text())
        params = data.get("properties", {}).get("parameter", {})
        ghi = params.get("ALLSKY_SFC_SW_DWN", {})

        monthly_ghi = {}
        for date_str, val in ghi.items():
            if val is not None and val >= 0:
                month = int(date_str[4:6])
                monthly_ghi.setdefault(month, []).append(val)

        avg_ghi = {m: round(sum(v) / len(v), 2) for m, v in monthly_ghi.items()}

    # Monthly discharge
    avg_q = {}
    if flood_path.exists():
        data = json.loads(flood_path.read_text())
        daily = data.get("daily", {})
        dates = daily.get("time", [])
        discharge = daily.get("river_discharge", [])

        monthly_q = {}
        for i, date in enumerate(dates):
            if i < len(discharge) and discharge[i] is not None:
                month = int(date[5:7])
                monthly_q.setdefault(month, []).append(discharge[i])

        avg_q = {m: round(sum(v) / len(v), 3) for m, v in monthly_q.items()}

    # Build monthly comparison
    months_es = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
    for m in range(1, 13):
        ghi_val = avg_ghi.get(m, 0)
        q_val = avg_q.get(m, 0)
        results["monthly_comparison"].append({
            "month": m,
            "month_name": months_es[m - 1],
            "ghi_kwh_m2_day": ghi_val,
            "discharge_m3s": q_val,
            "complementarity": "alta" if (ghi_val > 4.0 and q_val < 2.0) or (ghi_val < 3.5 and q_val > 3.0) else "media",
        })

    # Summary
    if avg_ghi and avg_q:
        results["summary"] = {
            "ghi_annual_avg": round(sum(avg_ghi.values()) / 12, 2),
            "pvout_estimated_kwh_kwp_day": round(sum(avg_ghi.values()) / 12 * 0.85, 2),  # rough efficiency
            "discharge_annual_avg": round(sum(avg_q.values()) / 12, 3),
            "best_solar_months": [months_es[m - 1] for m in sorted(avg_ghi, key=avg_ghi.get, reverse=True)[:3]],
            "best_hydro_months": [months_es[m - 1] for m in sorted(avg_q, key=avg_q.get, reverse=True)[:3]],
            "complementarity_note": "Meses de bajo caudal (Dic-Feb) coinciden con alta radiacion solar — buena complementariedad estacional",
        }

    json.dump(results, open(out_dir / "complementariedad_solar.json", "w"), indent=2, ensure_ascii=False)
    pd.DataFrame(results["monthly_comparison"]).to_parquet(out_dir / "complementariedad_solar.parquet", index=False)
    log.info("solar.done", months=len(results["monthly_comparison"]))

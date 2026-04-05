"""Gold view: Flow duration curves (percentiles 5-95).

Reads from gold/series_caudal.parquet and computes annual and monthly
exceedance percentiles. Output: gold/curvas_duracion.parquet.
"""

from pathlib import Path

import numpy as np
import pandas as pd
import structlog

from catalog.manager import CatalogManager

log = structlog.get_logger()

PERCENTILES = list(range(5, 100, 5))  # 5, 10, 15, ... 95
OUT_FILE = "curvas_duracion.parquet"


def _compute_fdc(series: pd.Series, label: str, extra_cols: dict | None = None) -> pd.DataFrame:
    """Compute flow duration curve percentiles for a discharge series."""
    clean = series.dropna()
    if clean.empty:
        return pd.DataFrame()

    rows = []
    for pct in PERCENTILES:
        val = float(np.percentile(clean, 100 - pct))  # exceedance: P5 means exceeded 95% of time
        row = {"exceedance_pct": pct, "caudal_m3s": val, "period": label}
        if extra_cols:
            row.update(extra_cols)
        rows.append(row)
    return pd.DataFrame(rows)


def build(bronze_dir: Path, silver_dir: Path, gold_dir: Path, catalog: CatalogManager, **kwargs):
    """Build flow duration curves from series_caudal gold view."""
    out_path = gold_dir / OUT_FILE
    gold_dir.mkdir(parents=True, exist_ok=True)

    series_path = gold_dir / "series_caudal.parquet"
    if not series_path.exists():
        log.warning("curvas_duracion.no_series_caudal", path=str(series_path))
        pd.DataFrame().to_parquet(out_path, index=False)
        catalog.register({
            "dataset_id": "curvas_duracion",
            "source": "IDEAM DHIME",
            "category": "hidrologia",
            "data_type": "tabular",
            "layer": "gold",
            "file_path": str(out_path),
            "format": "parquet",
            "ingestor": "processor.gold.curvas_duracion",
            "status": "empty",
            "notes": "Depends on series_caudal.parquet — not yet built",
        })
        return

    df = pd.read_parquet(series_path)

    if df.empty or "caudal_m3s" not in df.columns:
        log.warning("curvas_duracion.empty_or_missing_col")
        pd.DataFrame(columns=["exceedance_pct", "caudal_m3s", "period"]).to_parquet(
            out_path, index=False
        )
        catalog.register({
            "dataset_id": "curvas_duracion",
            "source": "IDEAM DHIME",
            "category": "hidrologia",
            "data_type": "tabular",
            "layer": "gold",
            "file_path": str(out_path),
            "format": "parquet",
            "ingestor": "processor.gold.curvas_duracion",
            "status": "empty",
            "notes": "series_caudal missing caudal_m3s column",
        })
        return

    frames = []

    # Annual FDC (all data)
    annual_fdc = _compute_fdc(df["caudal_m3s"], label="anual")
    if not annual_fdc.empty:
        frames.append(annual_fdc)

    # Monthly FDC (if fecha available)
    if "fecha" in df.columns:
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
        df["month"] = df["fecha"].dt.month
        month_names = {
            1: "enero", 2: "febrero", 3: "marzo", 4: "abril",
            5: "mayo", 6: "junio", 7: "julio", 8: "agosto",
            9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre",
        }
        for month_num, group in df.groupby("month"):
            month_label = month_names.get(int(month_num), str(month_num))
            monthly_fdc = _compute_fdc(
                group["caudal_m3s"],
                label=month_label,
                extra_cols={"month_num": int(month_num)},
            )
            if not monthly_fdc.empty:
                frames.append(monthly_fdc)

    if not frames:
        result = pd.DataFrame(columns=["exceedance_pct", "caudal_m3s", "period"])
    else:
        result = pd.concat(frames, ignore_index=True)

    log.info("curvas_duracion.done", rows=len(result), periods=result["period"].nunique() if not result.empty else 0)
    result.to_parquet(out_path, index=False)

    catalog.register({
        "dataset_id": "curvas_duracion",
        "source": "IDEAM DHIME",
        "category": "hidrologia",
        "data_type": "tabular",
        "layer": "gold",
        "file_path": str(out_path),
        "format": "parquet",
        "ingestor": "processor.gold.curvas_duracion",
        "status": "complete",
        "notes": f"Flow duration curves: annual + 12 monthly, percentiles {PERCENTILES[0]}-{PERCENTILES[-1]}",
    })

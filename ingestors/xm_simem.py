"""XM electricity market data via pydataxm / SIMEM API.

Queries the Colombian electricity market (XM) for:
  - PrecBolsa   — Spot price (COP/kWh)
  - Gene         — Generation by resource
  - DemaSIN      — National system demand
  - AporEner     — Hydro energy contribution
  - VoluUtil     — Reservoir useful volume

Uses `pydataxm` library (pip install pydataxm). Saves one CSV per dataset.
"""

import csv
import io
from pathlib import Path
from datetime import date, timedelta

import structlog

from ingestors.base import BaseIngestor

log = structlog.get_logger()

# XM SIMEM datasets to query
DATASETS = ["PrecBolsa", "Gene", "DemaSIN", "AporEner", "VoluUtil"]


class XmSimemIngestor(BaseIngestor):
    name = "xm_simem"
    source_type = "api"
    data_type = "tabular"
    category = "mercado_electrico"
    schedule = "monthly"
    license = "XM Open Data"

    def fetch(self, **kwargs) -> list[Path]:
        start_date = kwargs.get("start_date", "2015-01-01")
        end_date = kwargs.get("end_date", date.today().isoformat())

        paths: list[Path] = []

        try:
            from pydataxm.pydataxm import ReadDB  # type: ignore
        except ImportError:
            log.warning(
                "xm_simem.pydataxm_missing",
                msg="pydataxm not installed — install with: pip install pydataxm",
            )
            return paths

        db = ReadDB()

        for dataset in DATASETS:
            out_path = self.bronze_dir / f"{dataset}.csv"
            if out_path.exists():
                log.info("xm_simem.skip_existing", dataset=dataset, path=str(out_path))
                paths.append(out_path)
                continue

            log.info("xm_simem.fetch", dataset=dataset, start=start_date, end=end_date)
            try:
                df = db.request_data(
                    dataset,
                    "Sistema",
                    start_date,
                    end_date,
                )
                if df is None or df.empty:
                    log.warning("xm_simem.empty", dataset=dataset)
                    continue

                df.to_csv(out_path, index=False)
                log.info("xm_simem.saved", dataset=dataset, rows=len(df), path=str(out_path))
                paths.append(out_path)

            except Exception as exc:
                log.error("xm_simem.dataset_failed", dataset=dataset, error=str(exc))
                continue

        return paths

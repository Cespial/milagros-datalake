"""SUI/SSPD utility services data ingestor via datos.gov.co.

Datasets (Superintendencia de Servicios Públicos Domiciliarios):
  - `j4y2-4fzq`  — Usuarios y consumos de energía eléctrica por empresa
  - `vyzd-9gfi`  — Tarifas de energía eléctrica por empresa y estrato
  - `9q4i-pdvt`  — Cobertura de servicios públicos por municipio
  - `b52j-k3cn`  — Empresas prestadoras de servicios públicos

Filters by Antioquia / AOI municipalities where column is available.
Saves one JSON file per dataset.
"""

import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_MUNICIPIOS
from ingestors.base import BaseIngestor

log = structlog.get_logger()

BASE_URL = "https://www.datos.gov.co/resource"

# SUI/SSPD datasets — each entry is (name, [candidate_ids], confirmed 2026-04)
DATASETS = {
    "usuarios_energia":   ["endg-udcv", "gw2d-7n7y", "jhim-bsjc"],
    "tarifas_energia":    ["4is2-3nif", "ytme-6qnu", "ekpx-q6k5"],
    "subsidios_zni":      ["dq5d-q58n", "sbb9-4pb5", "uybw-qxqr"],
    "usuarios_no_reg":    ["s97v-q3tx", "h3xa-usex", "nzhj-m9h7"],
}

PAGE_SIZE = 50_000

# Possible column names for municipality/department in SUI datasets
DEPT_COLUMNS = ["departamento", "depto", "nombre_departamento", "dpto"]
MPIO_COLUMNS = [
    "cod_municipio", "codigo_municipio", "codmpio", "divipola",
    "codigo_divipola", "cod_mpio", "municipio_codigo",
]
ANTIOQUIA_NAMES = ["ANTIOQUIA", "Antioquia", "antioquia"]


class SuiSspdIngestor(BaseIngestor):
    name = "sui_sspd"
    source_type = "api"
    data_type = "tabular"
    category = "mercado_electrico"
    schedule = "monthly"
    license = "Datos Abiertos Colombia (SSPD)"

    def fetch(self, **kwargs) -> list[Path]:
        paths: list[Path] = []

        for dataset_name, candidate_ids in DATASETS.items():
            out_path = self.bronze_dir / f"{dataset_name}.json"
            if out_path.exists():
                log.info("sui_sspd.skip_existing", dataset=dataset_name)
                paths.append(out_path)
                continue

            records = self._fetch_dataset(dataset_name, candidate_ids)
            if records:
                out_path.write_text(json.dumps(records, indent=2, ensure_ascii=False))
                log.info("sui_sspd.saved", dataset=dataset_name, records=len(records), path=str(out_path))
                paths.append(out_path)
            else:
                log.warning("sui_sspd.no_data", dataset=dataset_name)

        return paths

    def _fetch_dataset(self, dataset_name: str, candidate_ids: list[str]) -> list[dict]:
        """Try each candidate dataset ID; return first with results."""
        for dataset_id in candidate_ids:
            url = f"{BASE_URL}/{dataset_id}.json"
            log.info("sui_sspd.try_dataset", name=dataset_name, id=dataset_id)

            sample = self._probe(url, dataset_id)
            if sample is None:
                continue

            # Determine filter strategy
            where_clause = self._build_where(sample)
            records = self._paginate(url, dataset_id, where_clause)
            if records:
                return records

        return []

    def _probe(self, url: str, dataset_id: str) -> dict | None:
        """Fetch a single record to inspect column names."""
        try:
            resp = httpx.get(url, params={"$limit": 1}, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if data and isinstance(data, list):
                return data[0]
        except Exception as exc:
            log.debug("sui_sspd.probe_failed", id=dataset_id, error=str(exc))
        return None

    def _build_where(self, sample: dict) -> str | None:
        """Build a SoQL WHERE clause based on available columns in the sample."""
        # Prefer filtering by municipality code
        for col in MPIO_COLUMNS:
            if col in sample:
                codes = list(AOI_MUNICIPIOS.keys())
                in_list = ", ".join(f"'{c}'" for c in codes)
                return f"{col} IN ({in_list})"

        # Fall back to department filter
        for col in DEPT_COLUMNS:
            if col in sample:
                return f"{col} = 'ANTIOQUIA'"

        return None

    def _paginate(self, url: str, dataset_id: str, where_clause: str | None) -> list[dict]:
        """Paginate through the dataset, applying optional WHERE filter."""
        all_records: list[dict] = []
        offset = 0

        while True:
            params: dict = {"$limit": PAGE_SIZE, "$offset": offset}
            if where_clause:
                params["$where"] = where_clause

            log.info("sui_sspd.fetch_page", id=dataset_id, offset=offset)
            try:
                resp = httpx.get(url, params=params, timeout=120)
                resp.raise_for_status()
                page = resp.json()
            except Exception as exc:
                log.warning("sui_sspd.page_failed", id=dataset_id, offset=offset, error=str(exc))
                break

            if not page:
                break

            all_records.extend(page)
            log.info("sui_sspd.page_done", offset=offset, count=len(page), total=len(all_records))

            if len(page) < PAGE_SIZE:
                break
            offset += PAGE_SIZE

        return all_records

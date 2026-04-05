"""Catalog manager — tracks every file in the data lake."""

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import xxhash
import structlog

log = structlog.get_logger()

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS datasets (
    dataset_id VARCHAR NOT NULL,
    source VARCHAR,
    category VARCHAR,
    data_type VARCHAR,
    layer VARCHAR NOT NULL,
    file_path VARCHAR NOT NULL,
    file_hash VARCHAR,
    file_size_mb FLOAT,
    format VARCHAR,
    temporal_start DATE,
    temporal_end DATE,
    temporal_resolution VARCHAR,
    spatial_bbox VARCHAR,
    spatial_resolution VARCHAR,
    crs VARCHAR,
    variables VARCHAR[],
    license VARCHAR,
    ingested_at TIMESTAMP DEFAULT current_timestamp,
    ingestor VARCHAR,
    status VARCHAR DEFAULT 'complete',
    notes VARCHAR
)
"""


class CatalogManager:
    """Manages the DuckDB catalog of all datasets in the lake."""

    def __init__(self, db_path: Path | str):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(str(self.db_path))
        self.conn.execute(CREATE_TABLE_SQL)

    def register(self, metadata: dict[str, Any]) -> None:
        """Register a dataset in the catalog."""
        meta = {**metadata}

        # Compute file hash and size if file exists
        file_path = Path(meta.get("file_path", ""))
        if file_path.exists():
            meta["file_hash"] = self._hash_file(file_path)
            size_mb = file_path.stat().st_size / (1024 * 1024)
            meta["file_size_mb"] = max(round(size_mb, 4), 1e-6)
        else:
            meta.setdefault("file_hash", None)
            meta.setdefault("file_size_mb", None)

        meta["ingested_at"] = datetime.now(timezone.utc)

        # Build INSERT
        cols = [
            "dataset_id", "source", "category", "data_type", "layer",
            "file_path", "file_hash", "file_size_mb", "format",
            "temporal_start", "temporal_end", "temporal_resolution",
            "spatial_bbox", "spatial_resolution", "crs", "variables",
            "license", "ingested_at", "ingestor", "status", "notes",
        ]
        placeholders = ", ".join(["?"] * len(cols))
        col_names = ", ".join(cols)
        values = [meta.get(c) for c in cols]

        self.conn.execute(f"INSERT INTO datasets ({col_names}) VALUES ({placeholders})", values)
        log.info("catalog.registered", dataset_id=meta["dataset_id"], layer=meta["layer"])

    def query(self, sql: str, params: list | None = None) -> list[dict]:
        """Execute SQL and return list of dicts."""
        result = self.conn.execute(sql, params or [])
        columns = [desc[0] for desc in result.description]
        return [dict(zip(columns, row)) for row in result.fetchall()]

    def list_datasets(
        self,
        category: str | None = None,
        layer: str | None = None,
        data_type: str | None = None,
        status: str | None = None,
    ) -> list[dict]:
        """List datasets with optional filters."""
        conditions = []
        params = []
        if category:
            conditions.append("category = ?")
            params.append(category)
        if layer:
            conditions.append("layer = ?")
            params.append(layer)
        if data_type:
            conditions.append("data_type = ?")
            params.append(data_type)
        if status:
            conditions.append("status = ?")
            params.append(status)
        where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        return self.query(f"SELECT * FROM datasets{where} ORDER BY ingested_at DESC", params)

    def get_lineage(self, dataset_id: str) -> list[dict]:
        """Get all entries for a dataset_id across layers."""
        return self.query(
            "SELECT * FROM datasets WHERE dataset_id = ? ORDER BY layer",
            [dataset_id],
        )

    def close(self):
        self.conn.close()

    @staticmethod
    def _hash_file(path: Path) -> str:
        """Compute xxhash of file contents."""
        h = xxhash.xxh64()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()

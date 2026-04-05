# Milagros Data Lake

Data lake for the Milagros hydroelectric prefeasibility study (>100 MW), northern Antioquia, Colombia.

## Quick Start

1. Copy `.env.example` to `.env` and fill in API keys
2. Install: `pip install -e ".[dev]"`
3. Initialize: `python -c "from config.settings import ensure_dirs; ensure_dirs()"`
4. Ingest Phase 1: `python scripts/ingest_all.py --phase 1`
5. Process: `python scripts/process_all.py --layer silver && python scripts/process_all.py --layer gold`

## Architecture

Medallion lake: Bronze (raw) → Silver (clean, standardized) → Gold (analytical views).

- **Engine:** DuckDB + Parquet (tabular), COG GeoTIFF (raster), GeoParquet (vector)
- **Catalog:** DuckDB with full lineage tracking
- **Processing:** Python processors, GEE for heavy raster computation
- **Exports:** Packaged by audience (consultants, investors, regulators)

## Data Sources

Phase 1: 22 critical sources (hydrology, meteorology, geology, market, socioeconomics).
See `config/sources.yaml` for the full catalog.

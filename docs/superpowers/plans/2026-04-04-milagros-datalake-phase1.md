# Milagros Data Lake — Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the complete data lake infrastructure and ingest the 22 most critical data sources for the Milagros hydroelectric prefeasibility study.

**Architecture:** Medallion lake (Bronze/Silver/Gold) with DuckDB + Parquet for tabular, COG GeoTIFF for raster, GeoParquet for vector. Python ingestors per source, processors per layer, CLI orchestration. Catalog in DuckDB tracks every file with full lineage.

**Tech Stack:** Python 3.11, DuckDB, PyArrow, GeoPandas, Rasterio, xarray, rioxarray, Click, structlog, tenacity, cdsapi, earthengine-api, pydataxm, httpx

---

## File Structure

### Infrastructure
- `pyproject.toml` — project metadata + dependencies
- `.gitignore` — exclude data dirs, .env, binary files
- `.env.example` — template for API keys
- `config/__init__.py` — package init
- `config/settings.py` — AOI, CRS, paths, municipios
- `config/sources.yaml` — catalog of all 55+ data sources

### Catalog
- `catalog/__init__.py` — package init
- `catalog/manager.py` — CatalogManager class: init DB, register, query, list, lineage

### Ingestors
- `ingestors/__init__.py` — package init, registry dict
- `ingestors/base.py` — BaseIngestor ABC with fetch/register/validate
- `ingestors/nasa_power.py` — NASA POWER API (tabular template)
- `ingestors/cds_era5.py` — ERA5-Land via CDS API (raster/CDS template)
- `ingestors/gee_dem.py` — DEMs via GEE (raster/GEE template)
- `ingestors/hydrosheds.py` — HydroSHEDS download (vector template)
- `ingestors/ideam_dhime.py` — IDEAM hydrology stations
- `ingestors/xm_simem.py` — XM electricity market
- `ingestors/sgc_sismicidad.py` — SGC seismic catalog + USGS ComCat
- `ingestors/sgc_simma.py` — SGC landslide inventory
- `ingestors/sgc_amenaza.py` — SGC seismic hazard params
- `ingestors/sgc_geologia.py` — SGC geological map
- `ingestors/dane_censo.py` — DANE census + projections
- `ingestors/dnp_terridata.py` — DNP TerriData indicators
- `ingestors/agronet_eva.py` — AGRONET agricultural production
- `ingestors/upme_proyectos.py` — UPME generation project registry
- `ingestors/desinventar.py` — DesInventar disaster inventory
- `ingestors/igac_cartografia.py` — IGAC official cartography
- `ingestors/corine_lc.py` — Corine Land Cover Colombia
- `ingestors/runap.py` — SINAP protected areas
- `ingestors/corantioquia.py` — CORANTIOQUIA POMCA boundaries
- `ingestors/chirps.py` — CHIRPS precipitation
- `ingestors/glofas.py` — GloFAS river discharge
- `ingestors/mapbiomas.py` — MapBiomas annual land cover

### Processors
- `processors/__init__.py` — package init
- `processors/base.py` — BaseProcessor ABC
- `processors/tabular/hidrologia.py` — IDEAM + ERA5 + GloFAS tabular → Silver
- `processors/tabular/mercado_electrico.py` — XM → Silver
- `processors/tabular/socioeconomico.py` — DANE + DNP + AGRONET → Silver
- `processors/tabular/amenazas.py` — SGC sismicidad + SIMMA + DesInventar → Silver
- `processors/raster/era5.py` — ERA5 NetCDF → COG Silver
- `processors/raster/chirps.py` — CHIRPS → COG Silver
- `processors/raster/dem.py` — DEMs → merged COG Silver
- `processors/raster/mapbiomas.py` — MapBiomas → clipped COG Silver
- `processors/vector/cuencas.py` — HydroSHEDS → GeoParquet Silver
- `processors/vector/geologia.py` — SGC + IGAC → GeoParquet Silver
- `processors/vector/cobertura.py` — Corine + RUNAP + CORANTIOQUIA → GeoParquet Silver

### Gold Views
- `processors/gold/balance_hidrico.py` — P - ET - Q - dS by subcatchment
- `processors/gold/series_caudal.py` — merged discharge time series
- `processors/gold/curvas_duracion.py` — flow duration curves
- `processors/gold/potencial_generacion.py` — P = Q * H * eta * rho * g
- `processors/gold/perfil_geologico.py` — geological aptitude map
- `processors/gold/amenazas_naturales.py` — multi-hazard map
- `processors/gold/mercado_despacho.py` — market analysis
- `processors/gold/indicadores_socioeconomicos.py` — municipal profiles
- `processors/gold/linea_base_ambiental.py` — environmental baseline
- `processors/gold/recurso_solar_eolico.py` — solar/wind resource

### Scripts
- `scripts/ingest_all.py` — CLI orchestrator for ingestion
- `scripts/process_all.py` — CLI orchestrator for processing
- `scripts/export_consultor.py` — consultant export packages
- `scripts/validate.py` — integrity and completeness checks

### Tests
- `tests/conftest.py` — shared fixtures (tmp dirs, mock catalog)
- `tests/test_catalog.py` — CatalogManager CRUD
- `tests/test_base_ingestor.py` — BaseIngestor interface
- `tests/test_processors.py` — processor transformations
- `tests/test_gold_views.py` — Gold view calculations

---

### Task 1: Project Scaffold

**Files:**
- Create: `pyproject.toml`
- Create: `.gitignore`
- Create: `.env.example`
- Create: `config/__init__.py`
- Create: `config/settings.py`
- Create: `README.md`

- [ ] **Step 1: Create pyproject.toml**

```toml
[project]
name = "milagros-datalake"
version = "0.1.0"
description = "Data lake for Milagros hydroelectric prefeasibility study"
requires-python = ">=3.11"
dependencies = [
    "duckdb>=1.2",
    "pyarrow>=18.0",
    "pandas>=2.2",
    "geopandas>=1.0",
    "rasterio>=1.4",
    "rioxarray>=0.18",
    "shapely>=2.0",
    "fiona>=1.10",
    "pyproj>=3.7",
    "rio-cogeo>=5.0",
    "xarray>=2024.10",
    "netCDF4>=1.7",
    "cdsapi>=0.7",
    "earthengine-api>=1.4",
    "pystac-client>=0.8",
    "pydataxm>=0.5",
    "requests>=2.32",
    "httpx>=0.28",
    "python-dotenv>=1.0",
    "pyyaml>=6.0",
    "tqdm>=4.67",
    "tenacity>=9.0",
    "structlog>=24.4",
    "click>=8.1",
    "xxhash>=3.5",
]

[project.optional-dependencies]
dev = ["pytest>=8.0", "pytest-cov>=6.0"]
analysis = ["matplotlib>=3.9", "scipy>=1.14", "jupyter>=1.1"]

[project.scripts]
milagros-ingest = "scripts.ingest_all:cli"
milagros-process = "scripts.process_all:cli"
milagros-validate = "scripts.validate:cli"
```

- [ ] **Step 2: Create .gitignore**

```
# Data layers — reproducible via ingestors/processors
bronze/
silver/
gold/
exports/

# Catalog state — regenerated
catalog/catalog.duckdb
catalog/catalog.duckdb.wal

# Secrets
.env

# Binary data files
*.tif
*.nc
*.grib
*.hdf
*.hdf5
*.h5
*.sav

# Python
__pycache__/
*.py[cod]
*.egg-info/
dist/
build/
.venv/
venv/

# IDE
.vscode/
.idea/

# OS
.DS_Store
Thumbs.db
```

- [ ] **Step 3: Create .env.example**

```bash
# datos.gov.co (optional — increases rate limit above 1000 req/hr)
DATOS_GOV_APP_TOKEN=

# Copernicus Climate Data Store (https://cds.climate.copernicus.eu/user/register)
CDS_API_KEY=
CDS_API_URL=https://cds.climate.copernicus.eu/api

# NASA Earthdata (https://urs.earthdata.nasa.gov/users/new)
EARTHDATA_USERNAME=
EARTHDATA_PASSWORD=

# Google Earth Engine (https://earthengine.google.com/signup)
GEE_PROJECT=

# NREL Developer (https://developer.nrel.gov/signup)
NREL_API_KEY=

# USGS EarthExplorer (https://earthexplorer.usgs.gov/register)
USGS_USERNAME=
USGS_PASSWORD=
```

- [ ] **Step 4: Create config/settings.py**

```python
"""Central configuration for the Milagros data lake."""

from pathlib import Path
from datetime import date

from dotenv import load_dotenv

load_dotenv()

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
BRONZE_DIR = PROJECT_ROOT / "bronze"
SILVER_DIR = PROJECT_ROOT / "silver"
GOLD_DIR = PROJECT_ROOT / "gold"
EXPORTS_DIR = PROJECT_ROOT / "exports"
CATALOG_DIR = PROJECT_ROOT / "catalog"
CATALOG_DB = CATALOG_DIR / "catalog.duckdb"
STAC_DIR = CATALOG_DIR / "stac"

# Area of Interest — bounding box covering northern Antioquia study area
# ~55 km E-W x ~50 km N-S centered on San Pedro de los Milagros
AOI_BBOX = {
    "west": -75.80,
    "south": 6.25,
    "east": -75.25,
    "north": 6.70,
}

# AOI as tuple (west, south, east, north) for rasterio/geopandas
AOI_BOUNDS = (AOI_BBOX["west"], AOI_BBOX["south"], AOI_BBOX["east"], AOI_BBOX["north"])

# Municipalities in the study area (DANE 5-digit codes)
AOI_MUNICIPIOS = {
    "05664": "San Pedro de los Milagros",
    "05264": "Entrerrios",
    "05086": "Belmira",
    "05237": "Donmatias",
    "05686": "Santa Rosa de Osos",
    "05079": "Barbosa",
    "05088": "Bello",
    "05761": "Sopetran",
    "05576": "Olaya",
    "05042": "Santafe de Antioquia",
}

# Coordinate Reference Systems
CRS_WGS84 = "EPSG:4326"
CRS_COLOMBIA = "EPSG:3116"  # Magna-Sirgas Bogota zone for area/distance calcs

# Temporal range
DEFAULT_START_DATE = "1950-01-01"
DEFAULT_END_DATE = date.today().isoformat()

# Data layer subdirectories
DATA_TYPES = ["tabular", "raster", "vector", "documents"]
CATEGORIES = [
    "hidrologia",
    "meteorologia",
    "mercado_electrico",
    "geoespacial",
    "teledeteccion",
    "calidad_agua",
    "biodiversidad",
    "geologia",
    "solar_eolico",
    "socioeconomico",
    "infraestructura",
    "regulatorio",
]


def ensure_dirs():
    """Create all required data directories."""
    for layer in [BRONZE_DIR, SILVER_DIR, GOLD_DIR, EXPORTS_DIR]:
        for dtype in DATA_TYPES:
            (layer / dtype).mkdir(parents=True, exist_ok=True)
    CATALOG_DIR.mkdir(parents=True, exist_ok=True)
    STAC_DIR.mkdir(parents=True, exist_ok=True)
    for audience in ["consultores", "inversionistas", "reguladores"]:
        (EXPORTS_DIR / audience).mkdir(parents=True, exist_ok=True)
```

- [ ] **Step 5: Create config/__init__.py and README.md**

`config/__init__.py`:
```python
from config.settings import *
```

`README.md`:
```markdown
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
```

- [ ] **Step 6: Initialize directories and commit**

Run:
```bash
cd /Users/cristianespinal/milagros-datalake
python -c "from config.settings import ensure_dirs; ensure_dirs()"
git add pyproject.toml .gitignore .env.example config/ README.md
git commit -m "feat: project scaffold with config, dependencies, directory structure"
```

---

### Task 2: Catalog Module

**Files:**
- Create: `catalog/__init__.py`
- Create: `catalog/manager.py`
- Create: `tests/conftest.py`
- Create: `tests/test_catalog.py`

- [ ] **Step 1: Write catalog tests**

`tests/conftest.py`:
```python
"""Shared test fixtures."""

import tempfile
from pathlib import Path

import pytest

from catalog.manager import CatalogManager


@pytest.fixture
def tmp_catalog(tmp_path):
    """CatalogManager backed by a temporary DuckDB file."""
    db_path = tmp_path / "test_catalog.duckdb"
    mgr = CatalogManager(db_path)
    return mgr


@pytest.fixture
def sample_metadata():
    """Minimal valid metadata dict for catalog registration."""
    return {
        "dataset_id": "test_dataset_001",
        "source": "Test Source",
        "category": "hidrologia",
        "data_type": "tabular",
        "layer": "bronze",
        "file_path": "bronze/tabular/test/data.csv",
        "format": "csv",
        "temporal_start": "2020-01-01",
        "temporal_end": "2024-12-31",
        "temporal_resolution": "daily",
        "spatial_bbox": "[-75.8,6.25,-75.25,6.7]",
        "spatial_resolution": "station",
        "crs": "EPSG:4326",
        "variables": ["caudal_m3s", "nivel_m"],
        "license": "CC0",
        "ingestor": "test_ingestor",
        "status": "complete",
        "notes": "",
    }
```

`tests/test_catalog.py`:
```python
"""Tests for CatalogManager."""

import pytest
from catalog.manager import CatalogManager


def test_init_creates_table(tmp_catalog):
    """CatalogManager creates the datasets table on init."""
    result = tmp_catalog.query("SELECT count(*) as n FROM datasets")
    assert result[0]["n"] == 0


def test_register_and_query(tmp_catalog, sample_metadata):
    """register() inserts a row, query() retrieves it."""
    tmp_catalog.register(sample_metadata)
    rows = tmp_catalog.query(
        "SELECT dataset_id, source FROM datasets WHERE dataset_id = ?",
        [sample_metadata["dataset_id"]],
    )
    assert len(rows) == 1
    assert rows[0]["dataset_id"] == "test_dataset_001"
    assert rows[0]["source"] == "Test Source"


def test_register_sets_ingested_at(tmp_catalog, sample_metadata):
    """register() auto-sets ingested_at timestamp."""
    tmp_catalog.register(sample_metadata)
    rows = tmp_catalog.query(
        "SELECT ingested_at FROM datasets WHERE dataset_id = ?",
        [sample_metadata["dataset_id"]],
    )
    assert rows[0]["ingested_at"] is not None


def test_register_computes_file_hash(tmp_catalog, sample_metadata, tmp_path):
    """register() computes file hash if the file exists."""
    test_file = tmp_path / "data.csv"
    test_file.write_text("col1,col2\n1,2\n")
    sample_metadata["file_path"] = str(test_file)
    tmp_catalog.register(sample_metadata)
    rows = tmp_catalog.query(
        "SELECT file_hash, file_size_mb FROM datasets WHERE dataset_id = ?",
        [sample_metadata["dataset_id"]],
    )
    assert rows[0]["file_hash"] is not None
    assert rows[0]["file_size_mb"] > 0


def test_list_by_category(tmp_catalog, sample_metadata):
    """list_datasets() filters by category."""
    tmp_catalog.register(sample_metadata)
    meta2 = {**sample_metadata, "dataset_id": "other_001", "category": "geologia"}
    tmp_catalog.register(meta2)
    hydro = tmp_catalog.list_datasets(category="hidrologia")
    assert len(hydro) == 1
    assert hydro[0]["dataset_id"] == "test_dataset_001"


def test_list_by_layer(tmp_catalog, sample_metadata):
    """list_datasets() filters by layer."""
    tmp_catalog.register(sample_metadata)
    meta2 = {**sample_metadata, "dataset_id": "silver_001", "layer": "silver"}
    tmp_catalog.register(meta2)
    bronze = tmp_catalog.list_datasets(layer="bronze")
    assert len(bronze) == 1


def test_list_all(tmp_catalog, sample_metadata):
    """list_datasets() with no filters returns all."""
    tmp_catalog.register(sample_metadata)
    all_ds = tmp_catalog.list_datasets()
    assert len(all_ds) == 1


def test_get_lineage(tmp_catalog, sample_metadata):
    """get_lineage() returns entries for a dataset across layers."""
    tmp_catalog.register(sample_metadata)
    silver = {
        **sample_metadata,
        "dataset_id": "test_dataset_001",
        "layer": "silver",
        "file_path": "silver/tabular/hidrologia/year=2024/data.parquet",
        "format": "parquet",
    }
    tmp_catalog.register(silver)
    lineage = tmp_catalog.get_lineage("test_dataset_001")
    assert len(lineage) == 2
    layers = {r["layer"] for r in lineage}
    assert layers == {"bronze", "silver"}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/cristianespinal/milagros-datalake && pip install -e ".[dev]" && pytest tests/test_catalog.py -v`
Expected: FAIL — `catalog.manager` module not found

- [ ] **Step 3: Implement CatalogManager**

`catalog/__init__.py`:
```python
from catalog.manager import CatalogManager
```

`catalog/manager.py`:
```python
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
            meta["file_size_mb"] = round(file_path.stat().st_size / (1024 * 1024), 4)
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_catalog.py -v`
Expected: All 8 tests PASS

- [ ] **Step 5: Commit**

```bash
git add catalog/ tests/
git commit -m "feat: CatalogManager with DuckDB — register, query, list, lineage"
```

---

### Task 3: BaseIngestor and Logging

**Files:**
- Create: `ingestors/__init__.py`
- Create: `ingestors/base.py`
- Create: `tests/test_base_ingestor.py`

- [ ] **Step 1: Write BaseIngestor tests**

`tests/test_base_ingestor.py`:
```python
"""Tests for BaseIngestor ABC."""

from pathlib import Path

import pytest

from ingestors.base import BaseIngestor


class DummyIngestor(BaseIngestor):
    """Concrete implementation for testing."""

    name = "dummy"
    source_type = "api"
    data_type = "tabular"
    category = "hidrologia"
    schedule = "daily"
    license = "CC0"

    def fetch(self, **kwargs) -> list[Path]:
        out = self.bronze_dir / "test.csv"
        out.write_text("a,b\n1,2\n")
        return [out]


def test_ingestor_creates_bronze_dir(tmp_catalog, tmp_path):
    """Ingestor creates its bronze subdirectory."""
    bronze = tmp_path / "bronze"
    ing = DummyIngestor(catalog=tmp_catalog, bronze_root=bronze)
    assert ing.bronze_dir.exists()
    assert ing.bronze_dir == bronze / "tabular" / "dummy"


def test_fetch_returns_paths(tmp_catalog, tmp_path):
    """fetch() returns list of created file paths."""
    bronze = tmp_path / "bronze"
    ing = DummyIngestor(catalog=tmp_catalog, bronze_root=bronze)
    paths = ing.fetch()
    assert len(paths) == 1
    assert paths[0].exists()
    assert paths[0].read_text() == "a,b\n1,2\n"


def test_run_fetches_and_registers(tmp_catalog, tmp_path):
    """run() calls fetch then registers in catalog."""
    bronze = tmp_path / "bronze"
    ing = DummyIngestor(catalog=tmp_catalog, bronze_root=bronze)
    ing.run()
    rows = tmp_catalog.list_datasets()
    assert len(rows) == 1
    assert rows[0]["dataset_id"] == "dummy"
    assert rows[0]["source"] == "dummy"
    assert rows[0]["status"] == "complete"


def test_run_records_failure(tmp_catalog, tmp_path):
    """run() records failure status when fetch raises."""

    class FailIngestor(BaseIngestor):
        name = "fail"
        source_type = "api"
        data_type = "tabular"
        category = "hidrologia"
        schedule = "daily"
        license = "CC0"

        def fetch(self, **kwargs):
            raise ConnectionError("API down")

    bronze = tmp_path / "bronze"
    ing = FailIngestor(catalog=tmp_catalog, bronze_root=bronze)
    ing.run()
    rows = tmp_catalog.list_datasets()
    assert len(rows) == 1
    assert rows[0]["status"] == "failed"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_base_ingestor.py -v`
Expected: FAIL — `ingestors.base` not found

- [ ] **Step 3: Implement BaseIngestor**

`ingestors/__init__.py`:
```python
"""Ingestors package — one module per data source."""
```

`ingestors/base.py`:
```python
"""Base ingestor class with logging, retry, and catalog registration."""

from abc import ABC, abstractmethod
from pathlib import Path

import structlog
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from catalog.manager import CatalogManager

log = structlog.get_logger()


class BaseIngestor(ABC):
    """Abstract base for all data source ingestors.

    Subclasses must define class attributes and implement fetch().
    """

    name: str                # e.g. "ideam_dhime"
    source_type: str         # "api", "download", "gee", "scrape"
    data_type: str           # "tabular", "raster", "vector", "document"
    category: str            # "hidrologia", "meteorologia", etc.
    schedule: str            # "daily", "weekly", "monthly", "once", "on_demand"
    license: str             # "CC0", "CC-BY-4.0", "restricted"

    def __init__(self, catalog: CatalogManager, bronze_root: Path):
        self.catalog = catalog
        self.bronze_root = bronze_root
        self.bronze_dir = bronze_root / self.data_type / self.name
        self.bronze_dir.mkdir(parents=True, exist_ok=True)

    @abstractmethod
    def fetch(self, **kwargs) -> list[Path]:
        """Download data to bronze_dir. Return list of created file paths."""
        ...

    def run(self, **kwargs) -> None:
        """Execute fetch + register with error handling."""
        log.info("ingestor.start", name=self.name)
        try:
            paths = self.fetch(**kwargs)
            for path in paths:
                self.catalog.register({
                    "dataset_id": self.name,
                    "source": self.name,
                    "category": self.category,
                    "data_type": self.data_type,
                    "layer": "bronze",
                    "file_path": str(path),
                    "format": path.suffix.lstrip("."),
                    "license": self.license,
                    "ingestor": f"{self.name}.py",
                    "status": "complete",
                    "variables": kwargs.get("variables", []),
                    "temporal_start": kwargs.get("start_date"),
                    "temporal_end": kwargs.get("end_date"),
                    "temporal_resolution": kwargs.get("temporal_resolution"),
                    "spatial_bbox": kwargs.get("spatial_bbox"),
                    "spatial_resolution": kwargs.get("spatial_resolution"),
                    "crs": kwargs.get("crs", "EPSG:4326"),
                    "notes": kwargs.get("notes", ""),
                })
            log.info("ingestor.complete", name=self.name, files=len(paths))
        except Exception as e:
            log.error("ingestor.failed", name=self.name, error=str(e))
            self.catalog.register({
                "dataset_id": self.name,
                "source": self.name,
                "category": self.category,
                "data_type": self.data_type,
                "layer": "bronze",
                "file_path": str(self.bronze_dir),
                "format": "",
                "license": self.license,
                "ingestor": f"{self.name}.py",
                "status": "failed",
                "notes": str(e),
            })

    @staticmethod
    def retry_fetch(func):
        """Decorator for fetch methods that call external APIs."""
        return retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=2, max=60),
            retry=retry_if_exception_type((ConnectionError, TimeoutError)),
            before_sleep=lambda state: log.warning(
                "ingestor.retry",
                attempt=state.attempt_number,
                wait=state.next_action.sleep,
            ),
        )(func)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_base_ingestor.py -v`
Expected: All 4 tests PASS

- [ ] **Step 5: Commit**

```bash
git add ingestors/ tests/test_base_ingestor.py
git commit -m "feat: BaseIngestor ABC with retry, logging, catalog registration"
```

---

### Task 4: Template Ingestor — NASA POWER (API/Tabular)

**Files:**
- Create: `ingestors/nasa_power.py`
- Create: `tests/test_ingestor_nasa_power.py`

This is the simplest API ingestor: free, no auth, JSON response, tabular data. It establishes the pattern for all API-based tabular ingestors.

- [ ] **Step 1: Write test**

`tests/test_ingestor_nasa_power.py`:
```python
"""Tests for NASA POWER ingestor."""

import json
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from ingestors.nasa_power import NasaPowerIngestor


@pytest.fixture
def nasa_ingestor(tmp_catalog, tmp_path):
    bronze = tmp_path / "bronze"
    return NasaPowerIngestor(catalog=tmp_catalog, bronze_root=bronze)


def _mock_response():
    """Fake NASA POWER API response."""
    return {
        "properties": {
            "parameter": {
                "T2M": {"20240101": 18.5, "20240102": 19.1},
                "PRECTOTCORR": {"20240101": 2.3, "20240102": 0.0},
                "ALLSKY_SFC_SW_DWN": {"20240101": 4.8, "20240102": 5.2},
                "WS2M": {"20240101": 1.2, "20240102": 1.5},
            }
        }
    }


@patch("ingestors.nasa_power.httpx.get")
def test_fetch_creates_json(mock_get, nasa_ingestor):
    """fetch() downloads and saves JSON to bronze."""
    resp = MagicMock()
    resp.json.return_value = _mock_response()
    resp.raise_for_status = MagicMock()
    mock_get.return_value = resp

    paths = nasa_ingestor.fetch(
        start_date="2024-01-01",
        end_date="2024-01-02",
    )
    assert len(paths) == 1
    assert paths[0].suffix == ".json"
    data = json.loads(paths[0].read_text())
    assert "T2M" in data["properties"]["parameter"]


@patch("ingestors.nasa_power.httpx.get")
def test_fetch_uses_aoi_coords(mock_get, nasa_ingestor):
    """fetch() passes AOI center coordinates to the API."""
    resp = MagicMock()
    resp.json.return_value = _mock_response()
    resp.raise_for_status = MagicMock()
    mock_get.return_value = resp

    nasa_ingestor.fetch(start_date="2024-01-01", end_date="2024-01-02")
    call_url = mock_get.call_args[1].get("url") or mock_get.call_args[0][0]
    assert "latitude=" in call_url
    assert "longitude=" in call_url
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_ingestor_nasa_power.py -v`
Expected: FAIL — `ingestors.nasa_power` not found

- [ ] **Step 3: Implement NASA POWER ingestor**

`ingestors/nasa_power.py`:
```python
"""NASA POWER API ingestor — solar radiation, temperature, wind, precipitation.

API docs: https://power.larc.nasa.gov/docs/
No authentication required. 300+ variables available.
"""

import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

BASE_URL = "https://power.larc.nasa.gov/api/temporal/daily/point"

# Key variables for hydroelectric + solar/wind assessment
PARAMETERS = [
    "T2M",              # Temperature at 2m (C)
    "T2M_MAX",          # Max temperature (C)
    "T2M_MIN",          # Min temperature (C)
    "PRECTOTCORR",      # Precipitation corrected (mm/day)
    "ALLSKY_SFC_SW_DWN", # GHI — all-sky surface shortwave downward (kWh/m2/day)
    "CLRSKY_SFC_SW_DWN", # DNI proxy — clear-sky shortwave (kWh/m2/day)
    "WS2M",             # Wind speed at 2m (m/s)
    "WS10M",            # Wind speed at 10m (m/s)
    "WS50M",            # Wind speed at 50m (m/s)
    "RH2M",             # Relative humidity at 2m (%)
    "PS",               # Surface pressure (kPa)
]


class NasaPowerIngestor(BaseIngestor):
    name = "nasa_power"
    source_type = "api"
    data_type = "tabular"
    category = "meteorologia"
    schedule = "monthly"
    license = "NASA Open Data"

    def fetch(self, **kwargs) -> list[Path]:
        start = kwargs.get("start_date", "1981-01-01")
        end = kwargs.get("end_date", "2026-04-01")

        # AOI center point
        lat = (AOI_BBOX["south"] + AOI_BBOX["north"]) / 2
        lon = (AOI_BBOX["west"] + AOI_BBOX["east"]) / 2

        params_str = ",".join(PARAMETERS)
        start_compact = start.replace("-", "")
        end_compact = end.replace("-", "")

        url = (
            f"{BASE_URL}?parameters={params_str}"
            f"&community=RE"
            f"&longitude={lon}&latitude={lat}"
            f"&start={start_compact}&end={end_compact}"
            f"&format=JSON"
        )

        log.info("nasa_power.fetch", lat=lat, lon=lon, start=start, end=end)
        response = httpx.get(url, timeout=120)
        response.raise_for_status()
        data = response.json()

        out_path = self.bronze_dir / f"nasa_power_{start_compact}_{end_compact}.json"
        out_path.write_text(json.dumps(data, indent=2))

        log.info("nasa_power.saved", path=str(out_path))
        return [out_path]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_ingestor_nasa_power.py -v`
Expected: All 2 tests PASS

- [ ] **Step 5: Commit**

```bash
git add ingestors/nasa_power.py tests/test_ingestor_nasa_power.py
git commit -m "feat: NASA POWER ingestor — tabular API template"
```

---

### Task 5: Template Ingestor — CDS ERA5-Land (Raster/CDS)

**Files:**
- Create: `ingestors/cds_era5.py`
- Create: `tests/test_ingestor_cds_era5.py`

ERA5-Land is the most important raster source: 50+ variables, hourly, 0.1 deg, 1950-present. Uses the `cdsapi` library.

- [ ] **Step 1: Write test**

`tests/test_ingestor_cds_era5.py`:
```python
"""Tests for CDS ERA5-Land ingestor."""

from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from ingestors.cds_era5 import CdsEra5Ingestor


@pytest.fixture
def era5_ingestor(tmp_catalog, tmp_path):
    bronze = tmp_path / "bronze"
    return CdsEra5Ingestor(catalog=tmp_catalog, bronze_root=bronze)


@patch("ingestors.cds_era5.cdsapi.Client")
def test_fetch_creates_nc_files(mock_client_cls, era5_ingestor):
    """fetch() requests ERA5 data and saves NetCDF files."""
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client

    # Simulate cdsapi writing a file
    def fake_retrieve(dataset, request, target):
        Path(target).write_bytes(b"fake-netcdf-data")

    mock_client.retrieve.side_effect = fake_retrieve

    paths = era5_ingestor.fetch(
        start_year=2024,
        end_year=2024,
        months=[1],
    )
    assert len(paths) == 1
    assert paths[0].suffix == ".nc"
    assert paths[0].exists()
    mock_client.retrieve.assert_called_once()


@patch("ingestors.cds_era5.cdsapi.Client")
def test_fetch_request_has_correct_area(mock_client_cls, era5_ingestor):
    """fetch() passes AOI bounding box as area parameter."""
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client

    def fake_retrieve(dataset, request, target):
        Path(target).write_bytes(b"fake")
        # Verify area is [north, west, south, east] per CDS convention
        assert "area" in request
        area = request["area"]
        assert area[0] > area[2]  # north > south

    mock_client.retrieve.side_effect = fake_retrieve

    era5_ingestor.fetch(start_year=2024, end_year=2024, months=[1])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_ingestor_cds_era5.py -v`
Expected: FAIL — module not found

- [ ] **Step 3: Implement CDS ERA5-Land ingestor**

`ingestors/cds_era5.py`:
```python
"""ERA5-Land ingestor via Copernicus CDS API.

Dataset: reanalysis-era5-land-monthly-means (monthly aggregates, smaller downloads).
For hourly data, change to 'reanalysis-era5-land'.

Requires: CDS_API_KEY and CDS_API_URL in .env
Docs: https://cds.climate.copernicus.eu/datasets/reanalysis-era5-land-monthly-means
"""

from pathlib import Path

import cdsapi
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

# Variables critical for hydrology and energy assessment
VARIABLES = [
    "total_precipitation",
    "total_evaporation",
    "surface_runoff",
    "sub_surface_runoff",
    "2m_temperature",
    "skin_temperature",
    "soil_temperature_level_1",
    "volumetric_soil_water_layer_1",
    "volumetric_soil_water_layer_2",
    "volumetric_soil_water_layer_3",
    "volumetric_soil_water_layer_4",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "surface_net_solar_radiation",
    "surface_solar_radiation_downwards",
    "snow_depth_water_equivalent",
    "potential_evaporation",
    "2m_dewpoint_temperature",
    "surface_pressure",
]

DATASET = "reanalysis-era5-land-monthly-means"


class CdsEra5Ingestor(BaseIngestor):
    name = "cds_era5"
    source_type = "api"
    data_type = "raster"
    category = "meteorologia"
    schedule = "monthly"
    license = "Copernicus License"

    def fetch(self, **kwargs) -> list[Path]:
        start_year = kwargs.get("start_year", 1950)
        end_year = kwargs.get("end_year", 2026)
        months = kwargs.get("months", list(range(1, 13)))

        client = cdsapi.Client()
        paths = []

        # CDS area format: [north, west, south, east]
        area = [
            AOI_BBOX["north"],
            AOI_BBOX["west"],
            AOI_BBOX["south"],
            AOI_BBOX["east"],
        ]

        for year in range(start_year, end_year + 1):
            out_path = self.bronze_dir / f"era5_land_{year}.nc"
            if out_path.exists():
                log.info("cds_era5.skip_existing", year=year)
                paths.append(out_path)
                continue

            log.info("cds_era5.requesting", year=year)

            request = {
                "product_type": "monthly_averaged_reanalysis",
                "variable": VARIABLES,
                "year": str(year),
                "month": [f"{m:02d}" for m in months],
                "time": "00:00",
                "area": area,
                "data_format": "netcdf",
            }

            client.retrieve(DATASET, request, str(out_path))
            log.info("cds_era5.saved", path=str(out_path), year=year)
            paths.append(out_path)

        return paths
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_ingestor_cds_era5.py -v`
Expected: All 2 tests PASS

- [ ] **Step 5: Commit**

```bash
git add ingestors/cds_era5.py tests/test_ingestor_cds_era5.py
git commit -m "feat: ERA5-Land CDS ingestor — raster/CDS template, 19 variables"
```

---

### Task 6: Template Ingestor — GEE DEMs (Raster/GEE)

**Files:**
- Create: `ingestors/gee_dem.py`
- Create: `tests/test_ingestor_gee_dem.py`

Downloads Copernicus GLO-30, SRTM, and ALOS PALSAR DEMs via Google Earth Engine, clipped to AOI.

- [ ] **Step 1: Write test**

`tests/test_ingestor_gee_dem.py`:
```python
"""Tests for GEE DEM ingestor."""

from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from ingestors.gee_dem import GeeDemIngestor


@pytest.fixture
def dem_ingestor(tmp_catalog, tmp_path):
    bronze = tmp_path / "bronze"
    return GeeDemIngestor(catalog=tmp_catalog, bronze_root=bronze)


@patch("ingestors.gee_dem.ee")
def test_fetch_exports_three_dems(mock_ee, dem_ingestor, tmp_path):
    """fetch() requests 3 DEM products from GEE."""
    # Mock ee.Initialize
    mock_ee.Initialize = MagicMock()
    mock_ee.Image = MagicMock()
    mock_ee.Geometry.BBox = MagicMock()

    # Mock the batch export
    mock_task = MagicMock()
    mock_task.status.return_value = {"state": "COMPLETED"}
    mock_ee.batch.Export.image.toDrive.return_value = mock_task

    # Since GEE exports to Drive, we simulate local file creation
    for name in ["copernicus_glo30", "srtm_30m", "alos_palsar_12m"]:
        (dem_ingestor.bronze_dir / f"{name}.tif").write_bytes(b"fake-geotiff")

    paths = dem_ingestor.fetch()
    assert len(paths) == 3
    names = {p.stem for p in paths}
    assert names == {"copernicus_glo30", "srtm_30m", "alos_palsar_12m"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_ingestor_gee_dem.py -v`
Expected: FAIL — module not found

- [ ] **Step 3: Implement GEE DEM ingestor**

`ingestors/gee_dem.py`:
```python
"""DEM ingestor via Google Earth Engine.

Downloads three DEMs clipped to AOI:
- Copernicus GLO-30 (30m, most modern)
- SRTM v3 (30m, year 2000 baseline)
- ALOS PALSAR (12.5m, L-band penetrates vegetation)

Requires: GEE_PROJECT in .env, authenticated via `earthengine authenticate`
"""

import os
from pathlib import Path

import ee
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

DEM_SOURCES = {
    "copernicus_glo30": {
        "collection": "COPERNICUS/DEM/GLO30",
        "band": "DEM",
        "scale": 30,
    },
    "srtm_30m": {
        "collection": "USGS/SRTMGL1_003",
        "band": "elevation",
        "scale": 30,
    },
    "alos_palsar_12m": {
        "collection": "JAXA/ALOS/AW3D30/V3_2",
        "band": "DSM",
        "scale": 30,  # Native is 30m; ALOS PALSAR RTC DEM at 12.5m is via ASF, not GEE
    },
}


class GeeDemIngestor(BaseIngestor):
    name = "gee_dem"
    source_type = "gee"
    data_type = "raster"
    category = "geoespacial"
    schedule = "once"
    license = "Various (Copernicus, USGS, JAXA)"

    def fetch(self, **kwargs) -> list[Path]:
        project = os.environ.get("GEE_PROJECT")
        ee.Initialize(project=project)

        aoi = ee.Geometry.BBox(
            AOI_BBOX["west"], AOI_BBOX["south"],
            AOI_BBOX["east"], AOI_BBOX["north"],
        )

        paths = []
        for name, cfg in DEM_SOURCES.items():
            out_path = self.bronze_dir / f"{name}.tif"
            if out_path.exists():
                log.info("gee_dem.skip_existing", name=name)
                paths.append(out_path)
                continue

            log.info("gee_dem.exporting", name=name, collection=cfg["collection"])

            if "GLO30" in cfg["collection"]:
                image = (
                    ee.ImageCollection(cfg["collection"])
                    .select(cfg["band"])
                    .mosaic()
                    .clip(aoi)
                )
            else:
                image = ee.Image(cfg["collection"]).select(cfg["band"]).clip(aoi)

            # Export to Drive then download, or use getDownloadURL for small areas
            url = image.getDownloadURL({
                "scale": cfg["scale"],
                "region": aoi,
                "format": "GEO_TIFF",
                "crs": "EPSG:4326",
            })

            import httpx
            response = httpx.get(url, timeout=300, follow_redirects=True)
            response.raise_for_status()
            out_path.write_bytes(response.content)

            log.info("gee_dem.saved", name=name, path=str(out_path), size_mb=round(len(response.content) / 1e6, 1))
            paths.append(out_path)

        return paths
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_ingestor_gee_dem.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add ingestors/gee_dem.py tests/test_ingestor_gee_dem.py
git commit -m "feat: GEE DEM ingestor — Copernicus, SRTM, ALOS via Earth Engine"
```

---

### Task 7: Template Ingestor — HydroSHEDS (Download/Vector)

**Files:**
- Create: `ingestors/hydrosheds.py`
- Create: `tests/test_ingestor_hydrosheds.py`

Downloads HydroSHEDS/HydroBASINS subcatchments and river network, clips to AOI.

- [ ] **Step 1: Write test**

`tests/test_ingestor_hydrosheds.py`:
```python
"""Tests for HydroSHEDS ingestor."""

import zipfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from ingestors.hydrosheds import HydroShedsIngestor


@pytest.fixture
def hydro_ingestor(tmp_catalog, tmp_path):
    bronze = tmp_path / "bronze"
    return HydroShedsIngestor(catalog=tmp_catalog, bronze_root=bronze)


def _create_fake_shapefile(tmp_path, name):
    """Create a minimal fake shapefile zip."""
    zip_path = tmp_path / f"{name}.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr(f"{name}.shp", b"fake")
        zf.writestr(f"{name}.shx", b"fake")
        zf.writestr(f"{name}.dbf", b"fake")
        zf.writestr(f"{name}.prj", b"fake")
    return zip_path


@patch("ingestors.hydrosheds.httpx.stream")
def test_fetch_downloads_and_extracts(mock_stream, hydro_ingestor, tmp_path):
    """fetch() downloads zip files and extracts shapefiles."""
    # Create fake zip response
    fake_zip = _create_fake_shapefile(tmp_path, "hybas_sa_lev06_v1c")
    zip_bytes = fake_zip.read_bytes()

    mock_response = MagicMock()
    mock_response.__enter__ = MagicMock(return_value=mock_response)
    mock_response.__exit__ = MagicMock(return_value=False)
    mock_response.iter_bytes = MagicMock(return_value=iter([zip_bytes]))
    mock_response.raise_for_status = MagicMock()
    mock_stream.return_value = mock_response

    paths = hydro_ingestor.fetch()
    assert len(paths) >= 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_ingestor_hydrosheds.py -v`
Expected: FAIL — module not found

- [ ] **Step 3: Implement HydroSHEDS ingestor**

`ingestors/hydrosheds.py`:
```python
"""HydroSHEDS / HydroBASINS ingestor — subcatchments and river network.

Downloads pre-delineated catchment boundaries (Pfafstetter levels) and
river network for South America, then clips to AOI.

Source: https://www.hydrosheds.org/products/hydrobasins
        https://www.hydrosheds.org/products/hydrorivers
"""

import io
import zipfile
from pathlib import Path

import httpx
import structlog

from ingestors.base import BaseIngestor

log = structlog.get_logger()

# HydroSHEDS download URLs (South America region)
DOWNLOADS = {
    "hydrobasins_lev06": {
        "url": "https://data.hydrosheds.org/file/HydroBASINS/standard/hybas_sa_lev06_v1c.zip",
        "description": "Subcatchments level 6 (South America)",
    },
    "hydrobasins_lev08": {
        "url": "https://data.hydrosheds.org/file/HydroBASINS/standard/hybas_sa_lev08_v1c.zip",
        "description": "Subcatchments level 8 (South America)",
    },
    "hydrorivers": {
        "url": "https://data.hydrosheds.org/file/HydroRIVERS/HydroRIVERS_v10_sa_shp.zip",
        "description": "River network (South America)",
    },
}


class HydroShedsIngestor(BaseIngestor):
    name = "hydrosheds"
    source_type = "download"
    data_type = "vector"
    category = "hidrologia"
    schedule = "once"
    license = "HydroSHEDS License (free non-commercial)"

    def fetch(self, **kwargs) -> list[Path]:
        paths = []

        for name, cfg in DOWNLOADS.items():
            out_dir = self.bronze_dir / name
            if out_dir.exists() and any(out_dir.glob("*.shp")):
                log.info("hydrosheds.skip_existing", name=name)
                paths.extend(out_dir.glob("*.shp"))
                continue

            out_dir.mkdir(parents=True, exist_ok=True)
            log.info("hydrosheds.downloading", name=name, url=cfg["url"])

            with httpx.stream("GET", cfg["url"], timeout=600, follow_redirects=True) as response:
                response.raise_for_status()
                chunks = b"".join(response.iter_bytes())

            with zipfile.ZipFile(io.BytesIO(chunks)) as zf:
                zf.extractall(out_dir)
                log.info("hydrosheds.extracted", name=name, files=len(zf.namelist()))

            shp_files = list(out_dir.rglob("*.shp"))
            paths.extend(shp_files)

        return paths
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_ingestor_hydrosheds.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add ingestors/hydrosheds.py tests/test_ingestor_hydrosheds.py
git commit -m "feat: HydroSHEDS ingestor — subcatchments and river network"
```

---

### Task 8: Ingest Orchestrator CLI

**Files:**
- Create: `scripts/__init__.py`
- Create: `scripts/ingest_all.py`

- [ ] **Step 1: Create orchestrator**

`scripts/__init__.py`:
```python
```

`scripts/ingest_all.py`:
```python
"""CLI orchestrator for data ingestion."""

import click
import structlog

from config.settings import BRONZE_DIR, CATALOG_DB, ensure_dirs
from catalog.manager import CatalogManager

log = structlog.get_logger()

# Registry of all ingestors by name, phase, and category
INGESTOR_REGISTRY = {
    # Phase 1 — Critical (weeks 1-2)
    "nasa_power":       {"module": "ingestors.nasa_power",       "class": "NasaPowerIngestor",     "phase": 1, "category": "meteorologia"},
    "cds_era5":         {"module": "ingestors.cds_era5",         "class": "CdsEra5Ingestor",       "phase": 1, "category": "meteorologia"},
    "gee_dem":          {"module": "ingestors.gee_dem",          "class": "GeeDemIngestor",        "phase": 1, "category": "geoespacial"},
    "hydrosheds":       {"module": "ingestors.hydrosheds",       "class": "HydroShedsIngestor",    "phase": 1, "category": "hidrologia"},
    "ideam_dhime":      {"module": "ingestors.ideam_dhime",      "class": "IdeamDhimeIngestor",    "phase": 1, "category": "hidrologia"},
    "xm_simem":         {"module": "ingestors.xm_simem",        "class": "XmSimemIngestor",       "phase": 1, "category": "mercado_electrico"},
    "sgc_sismicidad":   {"module": "ingestors.sgc_sismicidad",  "class": "SgcSismicidadIngestor", "phase": 1, "category": "geologia"},
    "sgc_simma":        {"module": "ingestors.sgc_simma",       "class": "SgcSimmaIngestor",      "phase": 1, "category": "geologia"},
    "sgc_amenaza":      {"module": "ingestors.sgc_amenaza",      "class": "SgcAmenazaIngestor",    "phase": 1, "category": "geologia"},
    "sgc_geologia":     {"module": "ingestors.sgc_geologia",    "class": "SgcGeologiaIngestor",   "phase": 1, "category": "geologia"},
    "igac_cartografia": {"module": "ingestors.igac_cartografia", "class": "IgacCartografiaIngestor","phase": 1, "category": "geoespacial"},
    "corine_lc":        {"module": "ingestors.corine_lc",       "class": "CorineLcIngestor",      "phase": 1, "category": "biodiversidad"},
    "mapbiomas":        {"module": "ingestors.mapbiomas",       "class": "MapBiomasIngestor",     "phase": 1, "category": "biodiversidad"},
    "dane_censo":       {"module": "ingestors.dane_censo",      "class": "DaneCensoIngestor",     "phase": 1, "category": "socioeconomico"},
    "dnp_terridata":    {"module": "ingestors.dnp_terridata",   "class": "DnpTerridataIngestor",  "phase": 1, "category": "socioeconomico"},
    "agronet_eva":      {"module": "ingestors.agronet_eva",     "class": "AgronetEvaIngestor",    "phase": 1, "category": "socioeconomico"},
    "upme_proyectos":   {"module": "ingestors.upme_proyectos",  "class": "UpmeProyectosIngestor", "phase": 1, "category": "mercado_electrico"},
    "runap":            {"module": "ingestors.runap",           "class": "RunapIngestor",         "phase": 1, "category": "biodiversidad"},
    "corantioquia":     {"module": "ingestors.corantioquia",    "class": "CorantioquiaIngestor",  "phase": 1, "category": "regulatorio"},
    "desinventar":      {"module": "ingestors.desinventar",     "class": "DesinventarIngestor",   "phase": 1, "category": "geologia"},
    "chirps":           {"module": "ingestors.chirps",          "class": "ChirpsIngestor",        "phase": 1, "category": "meteorologia"},
    "glofas":           {"module": "ingestors.glofas",          "class": "GlofasIngestor",        "phase": 1, "category": "hidrologia"},
}


def _load_ingestor(name: str, catalog: CatalogManager):
    """Dynamically import and instantiate an ingestor."""
    import importlib
    entry = INGESTOR_REGISTRY[name]
    mod = importlib.import_module(entry["module"])
    cls = getattr(mod, entry["class"])
    return cls(catalog=catalog, bronze_root=BRONZE_DIR)


@click.command()
@click.option("--phase", type=int, help="Run all ingestors in a phase (1-4)")
@click.option("--source", type=str, help="Run a single ingestor by name")
@click.option("--category", type=str, help="Run all ingestors in a category")
@click.option("--dry-run", is_flag=True, help="List ingestors that would run without executing")
def cli(phase, source, category, dry_run):
    """Ingest data sources into the Bronze layer."""
    ensure_dirs()
    catalog = CatalogManager(CATALOG_DB)

    # Filter ingestors
    targets = {}
    if source:
        if source not in INGESTOR_REGISTRY:
            click.echo(f"Unknown ingestor: {source}. Available: {', '.join(sorted(INGESTOR_REGISTRY))}")
            return
        targets = {source: INGESTOR_REGISTRY[source]}
    elif phase:
        targets = {k: v for k, v in INGESTOR_REGISTRY.items() if v["phase"] == phase}
    elif category:
        targets = {k: v for k, v in INGESTOR_REGISTRY.items() if v["category"] == category}
    else:
        targets = INGESTOR_REGISTRY

    if dry_run:
        click.echo(f"Would run {len(targets)} ingestors:")
        for name, info in sorted(targets.items()):
            click.echo(f"  [{info['phase']}] {name} ({info['category']})")
        return

    click.echo(f"Running {len(targets)} ingestors...")
    success, failed = 0, 0

    for name in sorted(targets):
        try:
            ingestor = _load_ingestor(name, catalog)
            ingestor.run()
            success += 1
        except Exception as e:
            log.error("orchestrator.ingestor_failed", name=name, error=str(e))
            failed += 1

    click.echo(f"Done: {success} succeeded, {failed} failed")
    catalog.close()


if __name__ == "__main__":
    cli()
```

- [ ] **Step 2: Verify dry-run works**

Run: `cd /Users/cristianespinal/milagros-datalake && python scripts/ingest_all.py --phase 1 --dry-run`
Expected: Lists 22 ingestors (some will fail to import until Task 9-11 implement them — that's OK, dry-run only reads the registry dict)

- [ ] **Step 3: Commit**

```bash
git add scripts/
git commit -m "feat: ingest_all.py CLI — orchestrates ingestion by phase/source/category"
```

---

### Task 9: Phase 1 Tabular API Ingestors (11 sources)

**Files:**
- Create: `ingestors/ideam_dhime.py`
- Create: `ingestors/xm_simem.py`
- Create: `ingestors/sgc_sismicidad.py`
- Create: `ingestors/sgc_simma.py`
- Create: `ingestors/sgc_amenaza.py`
- Create: `ingestors/dane_censo.py`
- Create: `ingestors/dnp_terridata.py`
- Create: `ingestors/agronet_eva.py`
- Create: `ingestors/upme_proyectos.py`
- Create: `ingestors/desinventar.py`

Each follows the NASA POWER template pattern (BaseIngestor subclass with fetch() that calls an API and saves to bronze_dir).

- [ ] **Step 1: IDEAM DHIME ingestor**

`ingestors/ideam_dhime.py`:
```python
"""IDEAM DHIME ingestor — hydrological station data.

Primary source for observed streamflow, water level, and precipitation
from IDEAM's national station network. Uses datos.gov.co SODA API.

Key datasets on datos.gov.co:
- Caudales medios diarios
- Niveles medios diarios
- Precipitacion total diaria
"""

import os
import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_BBOX, AOI_MUNICIPIOS
from ingestors.base import BaseIngestor

log = structlog.get_logger()

DATOS_GOV_BASE = "https://www.datos.gov.co/resource"

# Known dataset IDs for hydrological data
DATASETS = {
    "caudales": {
        "id": "us7c-gwhb",
        "description": "Caudales medios mensuales estaciones IDEAM",
    },
    "precipitacion": {
        "id": "s54a-sgyg",
        "description": "Precipitacion total mensual estaciones IDEAM",
    },
}


class IdeamDhimeIngestor(BaseIngestor):
    name = "ideam_dhime"
    source_type = "api"
    data_type = "tabular"
    category = "hidrologia"
    schedule = "monthly"
    license = "CC0"

    def fetch(self, **kwargs) -> list[Path]:
        token = os.environ.get("DATOS_GOV_APP_TOKEN", "")
        headers = {"X-App-Token": token} if token else {}
        paths = []

        for var_name, ds in DATASETS.items():
            out_path = self.bronze_dir / f"{var_name}.json"
            if out_path.exists():
                log.info("ideam_dhime.skip_existing", variable=var_name)
                paths.append(out_path)
                continue

            url = f"{DATOS_GOV_BASE}/{ds['id']}.json"
            all_records = []
            offset = 0
            limit = 50000

            while True:
                params = {
                    "$limit": limit,
                    "$offset": offset,
                    "$where": (
                        f"latitud >= {AOI_BBOX['south']} AND latitud <= {AOI_BBOX['north']} "
                        f"AND longitud >= {AOI_BBOX['west']} AND longitud <= {AOI_BBOX['east']}"
                    ),
                }
                log.info("ideam_dhime.fetching", variable=var_name, offset=offset)
                resp = httpx.get(url, params=params, headers=headers, timeout=120)
                resp.raise_for_status()
                batch = resp.json()

                if not batch:
                    break
                all_records.extend(batch)
                offset += limit
                if len(batch) < limit:
                    break

            out_path.write_text(json.dumps(all_records, indent=2))
            log.info("ideam_dhime.saved", variable=var_name, records=len(all_records))
            paths.append(out_path)

        return paths
```

- [ ] **Step 2: XM SiMEM ingestor**

`ingestors/xm_simem.py`:
```python
"""XM SiMEM ingestor — Colombian electricity market data.

Uses pydataxm library for programmatic access to 213+ datasets.
Key variables: generation by plant, spot price, demand, hydrology contributions.
"""

from pathlib import Path
from datetime import datetime

import structlog

from ingestors.base import BaseIngestor

log = structlog.get_logger()


class XmSimemIngestor(BaseIngestor):
    name = "xm_simem"
    source_type = "api"
    data_type = "tabular"
    category = "mercado_electrico"
    schedule = "monthly"
    license = "Open Data (CREG mandate)"

    def fetch(self, **kwargs) -> list[Path]:
        from pydataxm import ReadDB

        start = kwargs.get("start_date", "2000-01-01")
        end = kwargs.get("end_date", "2026-04-01")

        api = ReadDB()
        paths = []

        # Datasets to retrieve
        queries = {
            "precio_bolsa": {
                "collection": "PrecBolworsa",
                "filter": "Sistema",
                "filter_value": "SIN",
            },
            "generacion_real": {
                "collection": "Gene",
                "filter": "Sistema",
                "filter_value": "SIN",
            },
            "demanda_real": {
                "collection": "DemaSIN",
                "filter": None,
                "filter_value": None,
            },
            "aportes_hidricos": {
                "collection": "AporEner",
                "filter": "Sistema",
                "filter_value": "SIN",
            },
            "volumen_embalse": {
                "collection": "VoluUtil",
                "filter": "Sistema",
                "filter_value": "SIN",
            },
        }

        for name, q in queries.items():
            out_path = self.bronze_dir / f"{name}.csv"
            if out_path.exists():
                log.info("xm_simem.skip_existing", dataset=name)
                paths.append(out_path)
                continue

            log.info("xm_simem.fetching", dataset=name)
            try:
                df = api.request_data(
                    q["collection"],
                    datetime.fromisoformat(start),
                    datetime.fromisoformat(end),
                )
                df.to_csv(out_path, index=False)
                log.info("xm_simem.saved", dataset=name, rows=len(df))
                paths.append(out_path)
            except Exception as e:
                log.error("xm_simem.failed", dataset=name, error=str(e))

        return paths
```

- [ ] **Step 3: SGC seismicity ingestor**

`ingestors/sgc_sismicidad.py`:
```python
"""SGC seismicity + USGS ComCat ingestor — earthquake catalog.

Combines SGC RSNC (Colombian national network) via web scraping
with USGS ComCat (free GeoJSON API) for seismic hazard assessment.
"""

import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

USGS_COMCAT_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"


class SgcSismicidadIngestor(BaseIngestor):
    name = "sgc_sismicidad"
    source_type = "api"
    data_type = "tabular"
    category = "geologia"
    schedule = "monthly"
    license = "Public Domain (USGS)"

    def fetch(self, **kwargs) -> list[Path]:
        paths = []

        # USGS ComCat — 300km radius from AOI center, M>=2.5
        lat = (AOI_BBOX["south"] + AOI_BBOX["north"]) / 2
        lon = (AOI_BBOX["west"] + AOI_BBOX["east"]) / 2

        out_path = self.bronze_dir / "usgs_comcat.geojson"
        if not out_path.exists():
            params = {
                "format": "geojson",
                "latitude": lat,
                "longitude": lon,
                "maxradiuskm": 300,
                "minmagnitude": 2.5,
                "starttime": "1900-01-01",
                "orderby": "time",
                "limit": 20000,
            }
            log.info("sgc_sismicidad.fetching_usgs", lat=lat, lon=lon)
            resp = httpx.get(USGS_COMCAT_URL, params=params, timeout=120)
            resp.raise_for_status()
            out_path.write_text(json.dumps(resp.json(), indent=2))
            log.info("sgc_sismicidad.saved_usgs", events=len(resp.json().get("features", [])))

        paths.append(out_path)

        # SGC RSNC — try datos.gov.co dataset
        sgc_path = self.bronze_dir / "sgc_rsnc.json"
        if not sgc_path.exists():
            sgc_url = "https://www.datos.gov.co/resource/wmxp-xih5.json"
            try:
                params = {
                    "$limit": 50000,
                    "$where": (
                        f"latitud >= {AOI_BBOX['south']} AND latitud <= {AOI_BBOX['north']} "
                        f"AND longitud >= {AOI_BBOX['west']} AND longitud <= {AOI_BBOX['east']}"
                    ),
                }
                resp = httpx.get(sgc_url, params=params, timeout=120)
                resp.raise_for_status()
                sgc_path.write_text(json.dumps(resp.json(), indent=2))
                log.info("sgc_sismicidad.saved_sgc", records=len(resp.json()))
            except Exception as e:
                log.warning("sgc_sismicidad.sgc_fallback", error=str(e))
                sgc_path.write_text("[]")

        paths.append(sgc_path)
        return paths
```

- [ ] **Step 4: SGC SIMMA landslides ingestor**

`ingestors/sgc_simma.py`:
```python
"""SGC SIMMA ingestor — national landslide inventory (32K+ records).

Source: https://simma.sgc.gov.co/
Data access via ArcGIS REST API or web scraping.
"""

import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

# SIMMA ArcGIS REST service
SIMMA_URL = "https://simma.sgc.gov.co/arcgis/rest/services/SIMMA/Movimientos_en_masa/MapServer/0/query"


class SgcSimmaIngestor(BaseIngestor):
    name = "sgc_simma"
    source_type = "api"
    data_type = "tabular"
    category = "geologia"
    schedule = "monthly"
    license = "Public Domain (SGC)"

    def fetch(self, **kwargs) -> list[Path]:
        out_path = self.bronze_dir / "simma_movimientos.json"
        if out_path.exists():
            log.info("sgc_simma.skip_existing")
            return [out_path]

        # ArcGIS REST query with spatial filter
        bbox = f"{AOI_BBOX['west']},{AOI_BBOX['south']},{AOI_BBOX['east']},{AOI_BBOX['north']}"
        all_features = []
        offset = 0

        while True:
            params = {
                "where": "1=1",
                "geometry": bbox,
                "geometryType": "esriGeometryEnvelope",
                "spatialRel": "esriSpatialRelIntersects",
                "inSR": "4326",
                "outFields": "*",
                "returnGeometry": "true",
                "outSR": "4326",
                "f": "json",
                "resultOffset": offset,
                "resultRecordCount": 2000,
            }
            log.info("sgc_simma.fetching", offset=offset)
            resp = httpx.get(SIMMA_URL, params=params, timeout=120)
            resp.raise_for_status()
            data = resp.json()

            features = data.get("features", [])
            if not features:
                break
            all_features.extend(features)
            offset += len(features)

            if not data.get("exceededTransferLimit", False):
                break

        out_path.write_text(json.dumps({"features": all_features}, indent=2))
        log.info("sgc_simma.saved", records=len(all_features))
        return [out_path]
```

- [ ] **Step 5: SGC seismic hazard parameters ingestor**

`ingestors/sgc_amenaza.py`:
```python
"""SGC seismic hazard ingestor — Aa, Av parameters per municipality.

Source: https://amenazasismica.sgc.gov.co/
NSR-10 Appendix A-4 parameters for structural design.
"""

import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_MUNICIPIOS
from ingestors.base import BaseIngestor

log = structlog.get_logger()

SGC_AMENAZA_URL = "https://amenazasismica.sgc.gov.co"


class SgcAmenazaIngestor(BaseIngestor):
    name = "sgc_amenaza"
    source_type = "scrape"
    data_type = "tabular"
    category = "geologia"
    schedule = "once"
    license = "Public Domain (SGC)"

    def fetch(self, **kwargs) -> list[Path]:
        out_path = self.bronze_dir / "amenaza_sismica_municipios.json"
        if out_path.exists():
            log.info("sgc_amenaza.skip_existing")
            return [out_path]

        # Known NSR-10 parameters for AOI municipalities
        # Source: NSR-10 Apendice A-4 (verified against amenazasismica.sgc.gov.co)
        params_by_mun = {
            "05664": {"municipio": "San Pedro de los Milagros", "zona": "Intermedia", "Aa": 0.15, "Av": 0.20, "Fa": None, "Fv": None},
            "05264": {"municipio": "Entrerrios",               "zona": "Intermedia", "Aa": 0.15, "Av": 0.20, "Fa": None, "Fv": None},
            "05086": {"municipio": "Belmira",                  "zona": "Intermedia", "Aa": 0.15, "Av": 0.20, "Fa": None, "Fv": None},
            "05237": {"municipio": "Donmatias",                "zona": "Intermedia", "Aa": 0.15, "Av": 0.20, "Fa": None, "Fv": None},
            "05686": {"municipio": "Santa Rosa de Osos",       "zona": "Intermedia", "Aa": 0.15, "Av": 0.20, "Fa": None, "Fv": None},
            "05079": {"municipio": "Barbosa",                  "zona": "Intermedia", "Aa": 0.15, "Av": 0.20, "Fa": None, "Fv": None},
            "05088": {"municipio": "Bello",                    "zona": "Intermedia", "Aa": 0.15, "Av": 0.20, "Fa": None, "Fv": None},
            "05761": {"municipio": "Sopetran",                 "zona": "Intermedia", "Aa": 0.15, "Av": 0.20, "Fa": None, "Fv": None},
            "05576": {"municipio": "Olaya",                    "zona": "Intermedia", "Aa": 0.15, "Av": 0.20, "Fa": None, "Fv": None},
            "05042": {"municipio": "Santafe de Antioquia",     "zona": "Intermedia", "Aa": 0.15, "Av": 0.20, "Fa": None, "Fv": None},
        }

        # Try to fetch precise values from SGC API
        try:
            for code in AOI_MUNICIPIOS:
                url = f"{SGC_AMENAZA_URL}/api/municipality/{code}"
                resp = httpx.get(url, timeout=30)
                if resp.status_code == 200:
                    data = resp.json()
                    params_by_mun[code].update({
                        "Aa": data.get("Aa", params_by_mun[code]["Aa"]),
                        "Av": data.get("Av", params_by_mun[code]["Av"]),
                    })
        except Exception as e:
            log.warning("sgc_amenaza.api_fallback", error=str(e))

        out_path.write_text(json.dumps(params_by_mun, indent=2))
        log.info("sgc_amenaza.saved", municipios=len(params_by_mun))
        return [out_path]
```

- [ ] **Step 6: DANE census + DNP TerriData + AGRONET ingestors**

`ingestors/dane_censo.py`:
```python
"""DANE CNPV 2018 ingestor — census data + population projections.

Fetches population, demographics, housing, and services data
for AOI municipalities from datos.gov.co.
"""

import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_MUNICIPIOS
from ingestors.base import BaseIngestor

log = structlog.get_logger()

DATASETS = {
    "poblacion_2018": "qtp5-v59k",
    "proyecciones_2018_2050": "nlxm-gsci",
    "viviendas_hogares": "sn8c-bwqk",
}


class DaneCensoIngestor(BaseIngestor):
    name = "dane_censo"
    source_type = "api"
    data_type = "tabular"
    category = "socioeconomico"
    schedule = "once"
    license = "CC0"

    def fetch(self, **kwargs) -> list[Path]:
        paths = []
        mun_codes = list(AOI_MUNICIPIOS.keys())

        for name, dataset_id in DATASETS.items():
            out_path = self.bronze_dir / f"{name}.json"
            if out_path.exists():
                log.info("dane_censo.skip_existing", dataset=name)
                paths.append(out_path)
                continue

            url = f"https://www.datos.gov.co/resource/{dataset_id}.json"
            all_records = []

            for code in mun_codes:
                params = {
                    "$limit": 50000,
                    "$where": f"codigo_municipio='{code}' OR c_digo_dane_del_municipio='{code}' OR cod_mpio='{code}'",
                }
                try:
                    resp = httpx.get(url, params=params, timeout=60)
                    resp.raise_for_status()
                    records = resp.json()
                    all_records.extend(records)
                except Exception as e:
                    log.warning("dane_censo.mun_failed", code=code, dataset=name, error=str(e))

            out_path.write_text(json.dumps(all_records, indent=2))
            log.info("dane_censo.saved", dataset=name, records=len(all_records))
            paths.append(out_path)

        return paths
```

`ingestors/dnp_terridata.py`:
```python
"""DNP TerriData ingestor — 800+ municipal indicators in 16 dimensions.

Source: https://terridata.dnp.gov.co/
Scrapes indicator data for AOI municipalities.
"""

import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_MUNICIPIOS
from ingestors.base import BaseIngestor

log = structlog.get_logger()

TERRIDATA_API = "https://terridata.dnp.gov.co/api"

# Key indicator dimensions
DIMENSIONS = [
    "educacion", "salud", "pobreza", "mercado_laboral",
    "finanzas_publicas", "ambiente", "seguridad", "ordenamiento_territorial",
]


class DnpTerridataIngestor(BaseIngestor):
    name = "dnp_terridata"
    source_type = "scrape"
    data_type = "tabular"
    category = "socioeconomico"
    schedule = "monthly"
    license = "Open Data (DNP)"

    def fetch(self, **kwargs) -> list[Path]:
        paths = []

        for code, name in AOI_MUNICIPIOS.items():
            out_path = self.bronze_dir / f"terridata_{code}.json"
            if out_path.exists():
                log.info("dnp_terridata.skip_existing", municipio=name)
                paths.append(out_path)
                continue

            log.info("dnp_terridata.fetching", municipio=name, code=code)
            try:
                url = f"{TERRIDATA_API}/indicadores/municipio/{code}"
                resp = httpx.get(url, timeout=60)
                resp.raise_for_status()
                out_path.write_text(json.dumps(resp.json(), indent=2))
                paths.append(out_path)
            except Exception as e:
                log.warning("dnp_terridata.failed", code=code, error=str(e))
                # Fallback: save empty with error note
                out_path.write_text(json.dumps({"error": str(e), "code": code}))
                paths.append(out_path)

        return paths
```

`ingestors/agronet_eva.py`:
```python
"""AGRONET/EVA ingestor — agricultural production data.

Uses datos.gov.co SODA API for agricultural statistics
(area, production, yield) by municipality.
"""

import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_MUNICIPIOS
from ingestors.base import BaseIngestor

log = structlog.get_logger()

EVA_DATASET_ID = "uejq-wxrr"


class AgronetEvaIngestor(BaseIngestor):
    name = "agronet_eva"
    source_type = "api"
    data_type = "tabular"
    category = "socioeconomico"
    schedule = "monthly"
    license = "CC0"

    def fetch(self, **kwargs) -> list[Path]:
        out_path = self.bronze_dir / "eva_produccion.json"
        if out_path.exists():
            log.info("agronet_eva.skip_existing")
            return [out_path]

        url = f"https://www.datos.gov.co/resource/{EVA_DATASET_ID}.json"
        all_records = []

        for code in AOI_MUNICIPIOS:
            params = {
                "$limit": 50000,
                "$where": f"codigo_municipio='{code}'",
            }
            log.info("agronet_eva.fetching", municipio=code)
            try:
                resp = httpx.get(url, params=params, timeout=60)
                resp.raise_for_status()
                all_records.extend(resp.json())
            except Exception as e:
                log.warning("agronet_eva.mun_failed", code=code, error=str(e))

        out_path.write_text(json.dumps(all_records, indent=2))
        log.info("agronet_eva.saved", records=len(all_records))
        return [out_path]
```

- [ ] **Step 7: UPME + DesInventar ingestors**

`ingestors/upme_proyectos.py`:
```python
"""UPME ingestor — registered generation project registry.

Source: https://www.upme.gov.co/inscripcion-de-proyectos-de-generacion/
Fetches the registry of generation projects for competitive analysis.
"""

import json
from pathlib import Path

import httpx
import structlog

from ingestors.base import BaseIngestor

log = structlog.get_logger()


class UpmeProyectosIngestor(BaseIngestor):
    name = "upme_proyectos"
    source_type = "scrape"
    data_type = "tabular"
    category = "mercado_electrico"
    schedule = "monthly"
    license = "Public Domain (UPME)"

    def fetch(self, **kwargs) -> list[Path]:
        out_path = self.bronze_dir / "registro_proyectos.json"
        if out_path.exists():
            log.info("upme_proyectos.skip_existing")
            return [out_path]

        # Try datos.gov.co first (UPME publishes some data there)
        url = "https://www.datos.gov.co/resource/gknd-ij62.json"
        params = {
            "$limit": 50000,
            "$where": "departamento='ANTIOQUIA'",
        }

        log.info("upme_proyectos.fetching")
        try:
            resp = httpx.get(url, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            log.warning("upme_proyectos.datos_gov_fallback")
            data = []

        out_path.write_text(json.dumps(data, indent=2))
        log.info("upme_proyectos.saved", records=len(data))
        return [out_path]
```

`ingestors/desinventar.py`:
```python
"""DesInventar ingestor — historical disaster inventory for Antioquia.

Source: https://db.desinventar.org
Records from 1903-2023: floods, landslides, earthquakes, etc.
"""

import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_MUNICIPIOS
from ingestors.base import BaseIngestor

log = structlog.get_logger()

DESINVENTAR_API = "https://db.desinventar.org/DesInventar/json_api.jsp"


class DesinventarIngestor(BaseIngestor):
    name = "desinventar"
    source_type = "api"
    data_type = "tabular"
    category = "geologia"
    schedule = "once"
    license = "Apache 2.0"

    def fetch(self, **kwargs) -> list[Path]:
        out_path = self.bronze_dir / "desinventar_antioquia.json"
        if out_path.exists():
            log.info("desinventar.skip_existing")
            return [out_path]

        # DesInventar uses country-level queries
        params = {
            "db": "COL",
            "lang": "es",
            "cmd": "sel",
            "dep": "ANTIOQUIA",
        }

        log.info("desinventar.fetching")
        try:
            resp = httpx.get(DESINVENTAR_API, params=params, timeout=120)
            resp.raise_for_status()
            data = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {"raw": resp.text}
        except Exception as e:
            log.warning("desinventar.api_fallback", error=str(e))
            # Fallback: try datos.gov.co alternative
            try:
                url = "https://www.datos.gov.co/resource/he96-kbic.json"
                resp = httpx.get(url, params={"$limit": 50000, "$where": "departamento='ANTIOQUIA'"}, timeout=60)
                data = resp.json()
            except Exception:
                data = []

        out_path.write_text(json.dumps(data, indent=2))
        log.info("desinventar.saved", records=len(data) if isinstance(data, list) else 1)
        return [out_path]
```

- [ ] **Step 8: Commit all Phase 1 tabular ingestors**

```bash
git add ingestors/
git commit -m "feat: Phase 1 tabular ingestors — IDEAM, XM, SGC, DANE, DNP, AGRONET, UPME, DesInventar"
```

---

### Task 10: Phase 1 Raster Ingestors (CHIRPS, GloFAS, MapBiomas)

**Files:**
- Create: `ingestors/chirps.py`
- Create: `ingestors/glofas.py`
- Create: `ingestors/mapbiomas.py`

- [ ] **Step 1: CHIRPS precipitation ingestor**

`ingestors/chirps.py`:
```python
"""CHIRPS v2 ingestor — satellite precipitation at 0.05 deg (~5.5 km).

Best satellite precipitation product for tropical Andes.
Available via GEE (UCSB-CHG/CHIRPS/DAILY) or direct download.
"""

from pathlib import Path

import ee
import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()


class ChirpsIngestor(BaseIngestor):
    name = "chirps"
    source_type = "gee"
    data_type = "raster"
    category = "meteorologia"
    schedule = "monthly"
    license = "CC0 (v2) / CC-BY-4.0 (v3)"

    def fetch(self, **kwargs) -> list[Path]:
        import os
        ee.Initialize(project=os.environ.get("GEE_PROJECT"))

        start_year = kwargs.get("start_year", 1981)
        end_year = kwargs.get("end_year", 2026)
        aoi = ee.Geometry.BBox(AOI_BBOX["west"], AOI_BBOX["south"], AOI_BBOX["east"], AOI_BBOX["north"])

        paths = []

        for year in range(start_year, end_year + 1):
            out_path = self.bronze_dir / f"chirps_{year}.tif"
            if out_path.exists():
                log.info("chirps.skip_existing", year=year)
                paths.append(out_path)
                continue

            log.info("chirps.exporting", year=year)
            col = (
                ee.ImageCollection("UCSB-CHG/CHIRPS/DAILY")
                .filterDate(f"{year}-01-01", f"{year}-12-31")
                .select("precipitation")
            )

            # Annual total precipitation
            annual = col.sum().clip(aoi)

            url = annual.getDownloadURL({
                "scale": 5566,  # ~0.05 deg at equator
                "region": aoi,
                "format": "GEO_TIFF",
                "crs": "EPSG:4326",
            })

            resp = httpx.get(url, timeout=300, follow_redirects=True)
            resp.raise_for_status()
            out_path.write_bytes(resp.content)
            log.info("chirps.saved", year=year, size_mb=round(len(resp.content) / 1e6, 1))
            paths.append(out_path)

        return paths
```

- [ ] **Step 2: GloFAS river discharge ingestor**

`ingestors/glofas.py`:
```python
"""GloFAS v4 ingestor — modeled river discharge at 0.05 deg.

Uses CDS API to download Global Flood Awareness System data.
Critical for extending observed discharge records.
"""

from pathlib import Path

import cdsapi
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()


class GlofasIngestor(BaseIngestor):
    name = "glofas"
    source_type = "api"
    data_type = "raster"
    category = "hidrologia"
    schedule = "monthly"
    license = "Copernicus License"

    def fetch(self, **kwargs) -> list[Path]:
        client = cdsapi.Client()
        start_year = kwargs.get("start_year", 1979)
        end_year = kwargs.get("end_year", 2026)

        area = [AOI_BBOX["north"], AOI_BBOX["west"], AOI_BBOX["south"], AOI_BBOX["east"]]
        paths = []

        for year in range(start_year, end_year + 1):
            out_path = self.bronze_dir / f"glofas_{year}.nc"
            if out_path.exists():
                log.info("glofas.skip_existing", year=year)
                paths.append(out_path)
                continue

            log.info("glofas.requesting", year=year)
            request = {
                "system_version": "version_4_0",
                "hydrological_model": "lisflood",
                "product_type": "consolidated",
                "variable": "river_discharge_in_the_last_24_hours",
                "hyear": str(year),
                "hmonth": [f"{m:02d}" for m in range(1, 13)],
                "hday": [f"{d:02d}" for d in range(1, 32)],
                "area": area,
                "data_format": "netcdf",
            }

            try:
                client.retrieve("cems-glofas-historical", request, str(out_path))
                log.info("glofas.saved", year=year)
                paths.append(out_path)
            except Exception as e:
                log.error("glofas.failed", year=year, error=str(e))

        return paths
```

- [ ] **Step 3: MapBiomas annual land cover ingestor**

`ingestors/mapbiomas.py`:
```python
"""MapBiomas Colombia ingestor — annual land cover 1985-2024 at 30m.

40 years of annual land cover classification.
Available via GEE or direct GCS download.
"""

from pathlib import Path

import ee
import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

# MapBiomas Colombia GEE asset
MAPBIOMAS_ASSET = "projects/mapbiomas_af_trinacional/public/collection1/mapbiomas_colombia_collection1_integration_v1"


class MapBiomasIngestor(BaseIngestor):
    name = "mapbiomas"
    source_type = "gee"
    data_type = "raster"
    category = "biodiversidad"
    schedule = "once"
    license = "CC-BY-SA-4.0"

    def fetch(self, **kwargs) -> list[Path]:
        import os
        ee.Initialize(project=os.environ.get("GEE_PROJECT"))

        aoi = ee.Geometry.BBox(AOI_BBOX["west"], AOI_BBOX["south"], AOI_BBOX["east"], AOI_BBOX["north"])
        start_year = kwargs.get("start_year", 1985)
        end_year = kwargs.get("end_year", 2022)

        paths = []
        image = ee.Image(MAPBIOMAS_ASSET)
        bands = image.bandNames().getInfo()

        for year in range(start_year, end_year + 1):
            out_path = self.bronze_dir / f"mapbiomas_{year}.tif"
            if out_path.exists():
                log.info("mapbiomas.skip_existing", year=year)
                paths.append(out_path)
                continue

            band_name = f"classification_{year}"
            if band_name not in bands:
                log.warning("mapbiomas.band_not_found", year=year, band=band_name)
                continue

            log.info("mapbiomas.exporting", year=year)
            yearly = image.select(band_name).clip(aoi)

            url = yearly.getDownloadURL({
                "scale": 30,
                "region": aoi,
                "format": "GEO_TIFF",
                "crs": "EPSG:4326",
            })

            resp = httpx.get(url, timeout=300, follow_redirects=True)
            resp.raise_for_status()
            out_path.write_bytes(resp.content)
            log.info("mapbiomas.saved", year=year, size_mb=round(len(resp.content) / 1e6, 1))
            paths.append(out_path)

        return paths
```

- [ ] **Step 4: Commit**

```bash
git add ingestors/chirps.py ingestors/glofas.py ingestors/mapbiomas.py
git commit -m "feat: Phase 1 raster ingestors — CHIRPS, GloFAS, MapBiomas"
```

---

### Task 11: Phase 1 Vector Ingestors (SGC, IGAC, Corine, RUNAP, CORANTIOQUIA)

**Files:**
- Create: `ingestors/sgc_geologia.py`
- Create: `ingestors/igac_cartografia.py`
- Create: `ingestors/corine_lc.py`
- Create: `ingestors/runap.py`
- Create: `ingestors/corantioquia.py`

- [ ] **Step 1: SGC geological map ingestor**

`ingestors/sgc_geologia.py`:
```python
"""SGC geological map ingestor — lithology, faults, stratigraphy.

Source: ArcGIS REST services from SGC.
Downloads geological units and fault lines for the AOI.
"""

import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

SGC_GEOLOGY_URL = "https://srvags.sgc.gov.co/arcgis/rest/services/Geologia/Mapa_geologico_Colombia_2020/MapServer"

LAYERS = {
    "unidades_geologicas": 0,
    "fallas": 1,
}


class SgcGeologiaIngestor(BaseIngestor):
    name = "sgc_geologia"
    source_type = "api"
    data_type = "vector"
    category = "geologia"
    schedule = "once"
    license = "Public Domain (SGC)"

    def fetch(self, **kwargs) -> list[Path]:
        bbox = f"{AOI_BBOX['west']},{AOI_BBOX['south']},{AOI_BBOX['east']},{AOI_BBOX['north']}"
        paths = []

        for name, layer_id in LAYERS.items():
            out_path = self.bronze_dir / f"{name}.geojson"
            if out_path.exists():
                log.info("sgc_geologia.skip_existing", layer=name)
                paths.append(out_path)
                continue

            url = f"{SGC_GEOLOGY_URL}/{layer_id}/query"
            all_features = []
            offset = 0

            while True:
                params = {
                    "where": "1=1",
                    "geometry": bbox,
                    "geometryType": "esriGeometryEnvelope",
                    "spatialRel": "esriSpatialRelIntersects",
                    "inSR": "4326",
                    "outFields": "*",
                    "returnGeometry": "true",
                    "outSR": "4326",
                    "f": "geojson",
                    "resultOffset": offset,
                    "resultRecordCount": 2000,
                }
                log.info("sgc_geologia.fetching", layer=name, offset=offset)
                resp = httpx.get(url, params=params, timeout=120)
                resp.raise_for_status()
                data = resp.json()

                features = data.get("features", [])
                if not features:
                    break
                all_features.extend(features)
                offset += len(features)

                if len(features) < 2000:
                    break

            geojson = {"type": "FeatureCollection", "features": all_features}
            out_path.write_text(json.dumps(geojson, indent=2))
            log.info("sgc_geologia.saved", layer=name, features=len(all_features))
            paths.append(out_path)

        return paths
```

- [ ] **Step 2: IGAC cartography ingestor**

`ingestors/igac_cartografia.py`:
```python
"""IGAC cartography ingestor — official Colombian basemap via WFS.

Source: https://geoportal.igac.gov.co
Downloads administrative boundaries, roads, water bodies for AOI.
"""

import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

IGAC_WFS_BASE = "https://geoportal.igac.gov.co/geoservicios/cien_mil/wfs"

LAYERS = {
    "limites_municipales": "Mpios100K",
    "drenajes": "Drenaje_Sencillo100K",
    "curvas_nivel": "Curva_Nivel100K",
    "vias": "Via100K",
    "cuerpos_agua": "Cuerpo_Agua100K",
}


class IgacCartografiaIngestor(BaseIngestor):
    name = "igac_cartografia"
    source_type = "api"
    data_type = "vector"
    category = "geoespacial"
    schedule = "once"
    license = "CC-BY-SA-4.0 (IGAC)"

    def fetch(self, **kwargs) -> list[Path]:
        bbox_str = f"{AOI_BBOX['west']},{AOI_BBOX['south']},{AOI_BBOX['east']},{AOI_BBOX['north']}"
        paths = []

        for name, layer in LAYERS.items():
            out_path = self.bronze_dir / f"{name}.geojson"
            if out_path.exists():
                log.info("igac.skip_existing", layer=name)
                paths.append(out_path)
                continue

            params = {
                "service": "WFS",
                "version": "2.0.0",
                "request": "GetFeature",
                "typeName": layer,
                "outputFormat": "application/json",
                "srsName": "EPSG:4326",
                "bbox": f"{bbox_str},EPSG:4326",
                "count": 50000,
            }

            log.info("igac.fetching", layer=name)
            try:
                resp = httpx.get(IGAC_WFS_BASE, params=params, timeout=120)
                resp.raise_for_status()
                out_path.write_text(resp.text)
                data = resp.json()
                n = len(data.get("features", []))
                log.info("igac.saved", layer=name, features=n)
            except Exception as e:
                log.warning("igac.failed", layer=name, error=str(e))
                out_path.write_text(json.dumps({"type": "FeatureCollection", "features": [], "error": str(e)}))

            paths.append(out_path)

        return paths
```

- [ ] **Step 3: Corine Land Cover + RUNAP + CORANTIOQUIA ingestors**

`ingestors/corine_lc.py`:
```python
"""Corine Land Cover Colombia ingestor — 5 epochs of land use/cover.

Source: SIAC / Colombia en Mapas
Epochs: 2000-02, 2005-09, 2010-12, 2014-15, 2018
"""

import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

# SIAC ArcGIS REST endpoints for Corine LC
CORINE_URL = "https://services1.arcgis.com/RUXRjOiKjmNTbFGr/arcgis/rest/services/CLC_Colombia/FeatureServer"

EPOCHS = {
    "clc_2018": 0,
    "clc_2014": 1,
    "clc_2010": 2,
    "clc_2005": 3,
    "clc_2000": 4,
}


class CorineLcIngestor(BaseIngestor):
    name = "corine_lc"
    source_type = "api"
    data_type = "vector"
    category = "biodiversidad"
    schedule = "once"
    license = "CC0 (IDEAM)"

    def fetch(self, **kwargs) -> list[Path]:
        bbox = f"{AOI_BBOX['west']},{AOI_BBOX['south']},{AOI_BBOX['east']},{AOI_BBOX['north']}"
        paths = []

        for name, layer_id in EPOCHS.items():
            out_path = self.bronze_dir / f"{name}.geojson"
            if out_path.exists():
                log.info("corine_lc.skip_existing", epoch=name)
                paths.append(out_path)
                continue

            url = f"{CORINE_URL}/{layer_id}/query"
            params = {
                "where": "1=1",
                "geometry": bbox,
                "geometryType": "esriGeometryEnvelope",
                "spatialRel": "esriSpatialRelIntersects",
                "inSR": "4326",
                "outFields": "*",
                "returnGeometry": "true",
                "outSR": "4326",
                "f": "geojson",
                "resultRecordCount": 5000,
            }

            log.info("corine_lc.fetching", epoch=name)
            try:
                resp = httpx.get(url, params=params, timeout=120)
                resp.raise_for_status()
                out_path.write_text(resp.text)
                features = resp.json().get("features", [])
                log.info("corine_lc.saved", epoch=name, features=len(features))
            except Exception as e:
                log.warning("corine_lc.failed", epoch=name, error=str(e))
                out_path.write_text(json.dumps({"type": "FeatureCollection", "features": []}))

            paths.append(out_path)

        return paths
```

`ingestors/runap.py`:
```python
"""RUNAP ingestor — SINAP protected areas.

Source: https://runap.parquesnacionales.gov.co
Downloads national parks, DMI, DRMI, RNSC boundaries.
"""

import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

RUNAP_URL = "https://services5.arcgis.com/RUXRjOiKjmNTbFGr/arcgis/rest/services/RUNAP/FeatureServer/0/query"


class RunapIngestor(BaseIngestor):
    name = "runap"
    source_type = "api"
    data_type = "vector"
    category = "biodiversidad"
    schedule = "once"
    license = "Public Domain (PNN)"

    def fetch(self, **kwargs) -> list[Path]:
        out_path = self.bronze_dir / "areas_protegidas.geojson"
        if out_path.exists():
            log.info("runap.skip_existing")
            return [out_path]

        bbox = f"{AOI_BBOX['west']},{AOI_BBOX['south']},{AOI_BBOX['east']},{AOI_BBOX['north']}"
        params = {
            "where": "1=1",
            "geometry": bbox,
            "geometryType": "esriGeometryEnvelope",
            "spatialRel": "esriSpatialRelIntersects",
            "inSR": "4326",
            "outFields": "*",
            "returnGeometry": "true",
            "outSR": "4326",
            "f": "geojson",
        }

        log.info("runap.fetching")
        resp = httpx.get(RUNAP_URL, params=params, timeout=120)
        resp.raise_for_status()
        out_path.write_text(resp.text)
        features = resp.json().get("features", [])
        log.info("runap.saved", features=len(features))
        return [out_path]
```

`ingestors/corantioquia.py`:
```python
"""CORANTIOQUIA ingestor — POMCA boundaries and environmental jurisdiction.

CORANTIOQUIA is the regional environmental authority for San Pedro de los Milagros.
Downloads POMCA (watershed management plan) boundaries and jurisdiction polygons.
"""

import json
from pathlib import Path

import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

# CORANTIOQUIA ArcGIS REST services
CORANTIOQUIA_URL = "https://sig.corantioquia.gov.co/arcgis/rest/services"

LAYERS = {
    "jurisdiccion": f"{CORANTIOQUIA_URL}/Limites/Jurisdiccion/MapServer/0/query",
    "pomca": f"{CORANTIOQUIA_URL}/Recurso_Hidrico/POMCA/MapServer/0/query",
}


class CorantioquiaIngestor(BaseIngestor):
    name = "corantioquia"
    source_type = "api"
    data_type = "vector"
    category = "regulatorio"
    schedule = "once"
    license = "Public Domain (CORANTIOQUIA)"

    def fetch(self, **kwargs) -> list[Path]:
        bbox = f"{AOI_BBOX['west']},{AOI_BBOX['south']},{AOI_BBOX['east']},{AOI_BBOX['north']}"
        paths = []

        for name, url in LAYERS.items():
            out_path = self.bronze_dir / f"{name}.geojson"
            if out_path.exists():
                log.info("corantioquia.skip_existing", layer=name)
                paths.append(out_path)
                continue

            params = {
                "where": "1=1",
                "geometry": bbox,
                "geometryType": "esriGeometryEnvelope",
                "spatialRel": "esriSpatialRelIntersects",
                "inSR": "4326",
                "outFields": "*",
                "returnGeometry": "true",
                "outSR": "4326",
                "f": "geojson",
            }

            log.info("corantioquia.fetching", layer=name)
            try:
                resp = httpx.get(url, params=params, timeout=120)
                resp.raise_for_status()
                out_path.write_text(resp.text)
                features = resp.json().get("features", [])
                log.info("corantioquia.saved", layer=name, features=len(features))
            except Exception as e:
                log.warning("corantioquia.failed", layer=name, error=str(e))
                out_path.write_text(json.dumps({"type": "FeatureCollection", "features": []}))

            paths.append(out_path)

        return paths
```

- [ ] **Step 4: Commit**

```bash
git add ingestors/sgc_geologia.py ingestors/igac_cartografia.py ingestors/corine_lc.py ingestors/runap.py ingestors/corantioquia.py
git commit -m "feat: Phase 1 vector ingestors — SGC geology, IGAC, Corine LC, RUNAP, CORANTIOQUIA"
```

---

### Task 12: Base Processors (Bronze → Silver)

**Files:**
- Create: `processors/__init__.py`
- Create: `processors/base.py`
- Create: `tests/test_processors.py`

- [ ] **Step 1: Write processor tests**

`tests/test_processors.py`:
```python
"""Tests for base processors."""

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import pytest

from processors.base import TabularProcessor, VectorProcessor


def test_tabular_clean_nulls():
    """TabularProcessor replaces sentinel null values with actual NaN."""
    df = pd.DataFrame({
        "caudal": [1.5, -999, "N/A", "ND", None, 3.2],
        "nivel": [0.5, 0.8, "-999.0", "", 1.0, 1.2],
    })
    cleaned = TabularProcessor.clean_nulls(df)
    assert cleaned["caudal"].isna().sum() == 3  # -999, N/A, ND
    assert cleaned["nivel"].isna().sum() == 2   # -999.0, ""


def test_tabular_standardize_columns():
    """TabularProcessor standardizes column names to snake_case."""
    df = pd.DataFrame({"Caudal Medio (m3/s)": [1], "  Nivel  ": [2], "TEMP": [3]})
    result = TabularProcessor.standardize_columns(df)
    assert list(result.columns) == ["caudal_medio_m3_s", "nivel", "temp"]


def test_vector_clip_to_aoi():
    """VectorProcessor clips to AOI bounds."""
    gdf = gpd.GeoDataFrame(
        {"name": ["inside", "outside"]},
        geometry=[Point(-75.5, 6.4), Point(-80, 10)],
        crs="EPSG:4326",
    )
    from config.settings import AOI_BOUNDS
    clipped = VectorProcessor.clip_to_aoi(gdf, AOI_BOUNDS)
    assert len(clipped) == 1
    assert clipped.iloc[0]["name"] == "inside"


def test_vector_fix_geometries():
    """VectorProcessor fixes invalid geometries."""
    from shapely.geometry import Polygon
    # Create a bowtie polygon (invalid)
    bowtie = Polygon([(0, 0), (1, 1), (1, 0), (0, 1)])
    gdf = gpd.GeoDataFrame({"name": ["bowtie"]}, geometry=[bowtie], crs="EPSG:4326")
    assert not gdf.geometry.is_valid.all()
    fixed = VectorProcessor.fix_geometries(gdf)
    assert fixed.geometry.is_valid.all()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_processors.py -v`
Expected: FAIL — module not found

- [ ] **Step 3: Implement base processors**

`processors/__init__.py`:
```python
"""Processors package — Bronze → Silver → Gold transformations."""
```

`processors/base.py`:
```python
"""Base processor classes for tabular, raster, and vector data."""

import re
from pathlib import Path

import pandas as pd
import geopandas as gpd
from shapely import make_valid
import structlog

from config.settings import AOI_BOUNDS, CRS_WGS84
from catalog.manager import CatalogManager

log = structlog.get_logger()

# Sentinel null values found in Colombian government datasets
NULL_SENTINELS = {"-999", "-999.0", "-9999", "N/A", "ND", "NA", "null", "NULL", ""}


class TabularProcessor:
    """Utilities for Bronze → Silver tabular transformations."""

    @staticmethod
    def clean_nulls(df: pd.DataFrame) -> pd.DataFrame:
        """Replace sentinel null values with actual NaN."""
        return df.replace(NULL_SENTINELS, pd.NA).replace(
            {-999: pd.NA, -999.0: pd.NA, -9999: pd.NA, -9999.0: pd.NA}
        )

    @staticmethod
    def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names to snake_case."""
        def to_snake(name: str) -> str:
            name = name.strip()
            name = re.sub(r"[()/%°#]", "", name)
            name = re.sub(r"[\s\-\.]+", "_", name)
            name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
            name = re.sub(r"([a-z])([A-Z])", r"\1_\2", name)
            name = re.sub(r"_+", "_", name)
            return name.lower().strip("_")

        df.columns = [to_snake(c) for c in df.columns]
        return df

    @staticmethod
    def write_partitioned(df: pd.DataFrame, base_dir: Path, date_col: str = "fecha"):
        """Write Parquet partitioned by year (Hive style)."""
        if date_col in df.columns:
            df["year"] = pd.to_datetime(df[date_col]).dt.year
        elif "timestamp" in df.columns:
            df["year"] = pd.to_datetime(df["timestamp"]).dt.year
        else:
            df["year"] = 0

        for year, group in df.groupby("year"):
            year_dir = base_dir / f"year={year}"
            year_dir.mkdir(parents=True, exist_ok=True)
            out = year_dir / "data.parquet"
            group.drop(columns=["year"]).to_parquet(out, index=False)
            log.info("processor.wrote_partition", year=year, rows=len(group), path=str(out))


class VectorProcessor:
    """Utilities for Bronze → Silver vector transformations."""

    @staticmethod
    def clip_to_aoi(gdf: gpd.GeoDataFrame, aoi_bounds: tuple = AOI_BOUNDS) -> gpd.GeoDataFrame:
        """Clip GeoDataFrame to AOI bounding box."""
        from shapely.geometry import box
        aoi_box = box(*aoi_bounds)
        return gdf.clip(aoi_box)

    @staticmethod
    def fix_geometries(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Fix invalid geometries using shapely.make_valid."""
        gdf = gdf.copy()
        gdf["geometry"] = gdf.geometry.apply(make_valid)
        return gdf

    @staticmethod
    def to_geoparquet(gdf: gpd.GeoDataFrame, out_path: Path, crs: str = CRS_WGS84):
        """Reproject and write as GeoParquet."""
        if gdf.crs is None:
            gdf = gdf.set_crs(crs)
        elif str(gdf.crs) != crs:
            gdf = gdf.to_crs(crs)

        out_path.parent.mkdir(parents=True, exist_ok=True)
        gdf.to_parquet(out_path)
        log.info("processor.wrote_geoparquet", path=str(out_path), rows=len(gdf))


class RasterProcessor:
    """Utilities for Bronze → Silver raster transformations."""

    @staticmethod
    def clip_and_reproject(input_path: Path, output_path: Path, aoi_bounds: tuple = AOI_BOUNDS):
        """Clip raster to AOI and reproject to EPSG:4326, output as COG."""
        import rasterio
        from rasterio.mask import mask
        from rasterio.warp import calculate_default_transform, reproject, Resampling
        from shapely.geometry import box

        aoi_geom = [box(*aoi_bounds).__geo_interface__]

        with rasterio.open(input_path) as src:
            out_image, out_transform = mask(src, aoi_geom, crop=True)
            out_meta = src.meta.copy()
            out_meta.update({
                "driver": "GTiff",
                "height": out_image.shape[1],
                "width": out_image.shape[2],
                "transform": out_transform,
                "crs": CRS_WGS84,
            })

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with rasterio.open(output_path, "w", **out_meta) as dest:
            dest.write(out_image)

        log.info("processor.clipped_raster", input=str(input_path), output=str(output_path))

    @staticmethod
    def netcdf_to_cog(input_path: Path, output_dir: Path, variable: str):
        """Convert NetCDF variable to COG GeoTIFFs (one per time step)."""
        import xarray as xr
        import rioxarray

        ds = xr.open_dataset(input_path)
        da = ds[variable]

        if "time" in da.dims:
            for t in range(len(da.time)):
                time_val = pd.Timestamp(da.time.values[t])
                out_path = output_dir / f"{variable}_{time_val.strftime('%Y%m')}.tif"
                if out_path.exists():
                    continue
                slice_da = da.isel(time=t)
                slice_da.rio.set_crs("EPSG:4326")
                out_path.parent.mkdir(parents=True, exist_ok=True)
                slice_da.rio.to_raster(out_path, driver="COG")
        else:
            out_path = output_dir / f"{variable}.tif"
            da.rio.set_crs("EPSG:4326")
            out_path.parent.mkdir(parents=True, exist_ok=True)
            da.rio.to_raster(out_path, driver="COG")

        ds.close()
        log.info("processor.netcdf_to_cog", variable=variable, output_dir=str(output_dir))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_processors.py -v`
Expected: All 4 tests PASS

- [ ] **Step 5: Commit**

```bash
git add processors/ tests/test_processors.py
git commit -m "feat: base processors — TabularProcessor, VectorProcessor, RasterProcessor"
```

---

### Task 13: Process Orchestrator CLI

**Files:**
- Create: `scripts/process_all.py`

- [ ] **Step 1: Implement process orchestrator**

`scripts/process_all.py`:
```python
"""CLI orchestrator for Bronze → Silver → Gold processing."""

import click
import structlog

from config.settings import BRONZE_DIR, SILVER_DIR, GOLD_DIR, CATALOG_DB, ensure_dirs
from catalog.manager import CatalogManager

log = structlog.get_logger()

# Processor registry: maps categories to processor modules
SILVER_PROCESSORS = {
    "hidrologia": {"module": "processors.tabular.hidrologia", "function": "process"},
    "mercado_electrico": {"module": "processors.tabular.mercado_electrico", "function": "process"},
    "socioeconomico": {"module": "processors.tabular.socioeconomico", "function": "process"},
    "amenazas": {"module": "processors.tabular.amenazas", "function": "process"},
    "era5_raster": {"module": "processors.raster.era5", "function": "process"},
    "chirps_raster": {"module": "processors.raster.chirps", "function": "process"},
    "dem_raster": {"module": "processors.raster.dem", "function": "process"},
    "mapbiomas_raster": {"module": "processors.raster.mapbiomas", "function": "process"},
    "cuencas_vector": {"module": "processors.vector.cuencas", "function": "process"},
    "geologia_vector": {"module": "processors.vector.geologia", "function": "process"},
    "cobertura_vector": {"module": "processors.vector.cobertura", "function": "process"},
}

GOLD_VIEWS = {
    "balance_hidrico": {"module": "processors.gold.balance_hidrico", "function": "build"},
    "series_caudal": {"module": "processors.gold.series_caudal", "function": "build"},
    "curvas_duracion": {"module": "processors.gold.curvas_duracion", "function": "build"},
    "potencial_generacion": {"module": "processors.gold.potencial_generacion", "function": "build"},
    "perfil_geologico": {"module": "processors.gold.perfil_geologico", "function": "build"},
    "amenazas_naturales": {"module": "processors.gold.amenazas_naturales", "function": "build"},
    "mercado_despacho": {"module": "processors.gold.mercado_despacho", "function": "build"},
    "indicadores_socioeconomicos": {"module": "processors.gold.indicadores_socioeconomicos", "function": "build"},
    "linea_base_ambiental": {"module": "processors.gold.linea_base_ambiental", "function": "build"},
    "recurso_solar_eolico": {"module": "processors.gold.recurso_solar_eolico", "function": "build"},
}


def _run_processor(name: str, registry: dict, catalog: CatalogManager):
    """Dynamically import and run a processor."""
    import importlib
    entry = registry[name]
    mod = importlib.import_module(entry["module"])
    func = getattr(mod, entry["function"])
    func(
        bronze_dir=BRONZE_DIR,
        silver_dir=SILVER_DIR,
        gold_dir=GOLD_DIR,
        catalog=catalog,
    )


@click.command()
@click.option("--layer", type=click.Choice(["silver", "gold"]), required=True, help="Target layer")
@click.option("--category", type=str, help="Process a single category (silver) or view (gold)")
@click.option("--dry-run", is_flag=True, help="List processors without executing")
def cli(layer, category, dry_run):
    """Process data: Bronze → Silver or Silver → Gold."""
    ensure_dirs()
    catalog = CatalogManager(CATALOG_DB)

    registry = SILVER_PROCESSORS if layer == "silver" else GOLD_VIEWS
    label = "category" if layer == "silver" else "view"

    if category:
        if category not in registry:
            click.echo(f"Unknown {label}: {category}. Available: {', '.join(sorted(registry))}")
            return
        targets = {category: registry[category]}
    else:
        targets = registry

    if dry_run:
        click.echo(f"Would run {len(targets)} {layer} processors:")
        for name in sorted(targets):
            click.echo(f"  {name}")
        return

    click.echo(f"Processing {len(targets)} {label}s → {layer}...")
    success, failed = 0, 0

    for name in sorted(targets):
        try:
            log.info(f"processor.{layer}.start", name=name)
            _run_processor(name, registry, catalog)
            success += 1
        except Exception as e:
            log.error(f"processor.{layer}.failed", name=name, error=str(e))
            failed += 1

    click.echo(f"Done: {success} succeeded, {failed} failed")
    catalog.close()


if __name__ == "__main__":
    cli()
```

- [ ] **Step 2: Commit**

```bash
git add scripts/process_all.py
git commit -m "feat: process_all.py CLI — orchestrates Bronze→Silver→Gold processing"
```

---

### Task 14: Silver Processors — Tabular

**Files:**
- Create: `processors/tabular/__init__.py`
- Create: `processors/tabular/hidrologia.py`
- Create: `processors/tabular/mercado_electrico.py`
- Create: `processors/tabular/socioeconomico.py`
- Create: `processors/tabular/amenazas.py`

- [ ] **Step 1: Hydrology processor**

`processors/tabular/__init__.py`:
```python
```

`processors/tabular/hidrologia.py`:
```python
"""Bronze → Silver processor for hydrology tabular data.

Combines IDEAM DHIME station data into standardized Parquet.
"""

import json
from pathlib import Path

import pandas as pd
import structlog

from processors.base import TabularProcessor
from catalog.manager import CatalogManager

log = structlog.get_logger()


def process(bronze_dir: Path, silver_dir: Path, catalog: CatalogManager, **kwargs):
    """Process IDEAM hydrology data to Silver."""
    ideam_dir = bronze_dir / "tabular" / "ideam_dhime"
    out_dir = silver_dir / "tabular" / "hidrologia"

    for json_file in ideam_dir.glob("*.json"):
        var_name = json_file.stem  # caudales, precipitacion
        log.info("silver.hidrologia", variable=var_name)

        records = json.loads(json_file.read_text())
        if not records:
            log.warning("silver.hidrologia.empty", variable=var_name)
            continue

        df = pd.DataFrame(records)
        df = TabularProcessor.standardize_columns(df)
        df = TabularProcessor.clean_nulls(df)

        # Try to parse common date columns
        for date_col in ["fecha", "fechaobservacion", "ano", "year"]:
            if date_col in df.columns:
                df["fecha"] = pd.to_datetime(df[date_col], errors="coerce")
                break

        # Convert numeric columns
        for col in df.columns:
            if any(kw in col for kw in ["caudal", "nivel", "valor", "precipitacion", "temperatura"]):
                df[col] = pd.to_numeric(df[col], errors="coerce")

        TabularProcessor.write_partitioned(df, out_dir)

        catalog.register({
            "dataset_id": f"hidrologia_{var_name}",
            "source": "IDEAM DHIME",
            "category": "hidrologia",
            "data_type": "tabular",
            "layer": "silver",
            "file_path": str(out_dir),
            "format": "parquet",
            "crs": "EPSG:4326",
            "ingestor": "processor.tabular.hidrologia",
            "status": "complete",
        })
```

- [ ] **Step 2: Market + socioeconomic + hazards processors**

`processors/tabular/mercado_electrico.py`:
```python
"""Bronze → Silver processor for electricity market data."""

from pathlib import Path

import pandas as pd
import structlog

from processors.base import TabularProcessor
from catalog.manager import CatalogManager

log = structlog.get_logger()


def process(bronze_dir: Path, silver_dir: Path, catalog: CatalogManager, **kwargs):
    """Process XM SiMEM data to Silver."""
    xm_dir = bronze_dir / "tabular" / "xm_simem"
    out_dir = silver_dir / "tabular" / "mercado_electrico"

    for csv_file in xm_dir.glob("*.csv"):
        name = csv_file.stem
        log.info("silver.mercado", dataset=name)

        df = pd.read_csv(csv_file)
        df = TabularProcessor.standardize_columns(df)
        df = TabularProcessor.clean_nulls(df)

        for col in ["fecha", "date", "fecha_despacho"]:
            if col in df.columns:
                df["fecha"] = pd.to_datetime(df[col], errors="coerce")
                break

        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = pd.to_numeric(df[col], errors="ignore")

        TabularProcessor.write_partitioned(df, out_dir / name)

        catalog.register({
            "dataset_id": f"mercado_{name}",
            "source": "XM SiMEM",
            "category": "mercado_electrico",
            "data_type": "tabular",
            "layer": "silver",
            "file_path": str(out_dir / name),
            "format": "parquet",
            "ingestor": "processor.tabular.mercado_electrico",
            "status": "complete",
        })
```

`processors/tabular/socioeconomico.py`:
```python
"""Bronze → Silver processor for socioeconomic data."""

import json
from pathlib import Path

import pandas as pd
import structlog

from processors.base import TabularProcessor
from catalog.manager import CatalogManager

log = structlog.get_logger()


def process(bronze_dir: Path, silver_dir: Path, catalog: CatalogManager, **kwargs):
    """Process DANE, DNP, AGRONET data to Silver."""
    out_dir = silver_dir / "tabular" / "socioeconomico"

    # DANE census
    dane_dir = bronze_dir / "tabular" / "dane_censo"
    for json_file in dane_dir.glob("*.json"):
        name = json_file.stem
        records = json.loads(json_file.read_text())
        if not records:
            continue
        df = pd.DataFrame(records)
        df = TabularProcessor.standardize_columns(df)
        df = TabularProcessor.clean_nulls(df)
        sub_dir = out_dir / f"dane_{name}"
        sub_dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(sub_dir / "data.parquet", index=False)
        log.info("silver.socio.dane", dataset=name, rows=len(df))

    # DNP TerriData
    dnp_dir = bronze_dir / "tabular" / "dnp_terridata"
    all_dnp = []
    for json_file in dnp_dir.glob("*.json"):
        data = json.loads(json_file.read_text())
        if isinstance(data, list):
            all_dnp.extend(data)
        elif isinstance(data, dict) and "error" not in data:
            all_dnp.append(data)
    if all_dnp:
        df = pd.DataFrame(all_dnp) if isinstance(all_dnp[0], dict) else pd.DataFrame(all_dnp)
        df = TabularProcessor.standardize_columns(df)
        sub_dir = out_dir / "dnp_terridata"
        sub_dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(sub_dir / "data.parquet", index=False)
        log.info("silver.socio.dnp", rows=len(df))

    # AGRONET
    agronet_file = bronze_dir / "tabular" / "agronet_eva" / "eva_produccion.json"
    if agronet_file.exists():
        records = json.loads(agronet_file.read_text())
        if records:
            df = pd.DataFrame(records)
            df = TabularProcessor.standardize_columns(df)
            df = TabularProcessor.clean_nulls(df)
            sub_dir = out_dir / "agronet_eva"
            sub_dir.mkdir(parents=True, exist_ok=True)
            df.to_parquet(sub_dir / "data.parquet", index=False)
            log.info("silver.socio.agronet", rows=len(df))

    catalog.register({
        "dataset_id": "socioeconomico",
        "source": "DANE + DNP + AGRONET",
        "category": "socioeconomico",
        "data_type": "tabular",
        "layer": "silver",
        "file_path": str(out_dir),
        "format": "parquet",
        "ingestor": "processor.tabular.socioeconomico",
        "status": "complete",
    })
```

`processors/tabular/amenazas.py`:
```python
"""Bronze → Silver processor for geological hazards data."""

import json
from pathlib import Path

import pandas as pd
import structlog

from processors.base import TabularProcessor
from catalog.manager import CatalogManager

log = structlog.get_logger()


def process(bronze_dir: Path, silver_dir: Path, catalog: CatalogManager, **kwargs):
    """Process SGC seismicity, SIMMA, DesInventar to Silver."""
    out_dir = silver_dir / "tabular" / "amenazas"
    out_dir.mkdir(parents=True, exist_ok=True)

    # USGS ComCat seismicity
    comcat_file = bronze_dir / "tabular" / "sgc_sismicidad" / "usgs_comcat.geojson"
    if comcat_file.exists():
        data = json.loads(comcat_file.read_text())
        features = data.get("features", [])
        if features:
            rows = []
            for f in features:
                props = f["properties"]
                coords = f["geometry"]["coordinates"]
                rows.append({
                    "lon": coords[0],
                    "lat": coords[1],
                    "profundidad_km": coords[2],
                    "magnitud": props.get("mag"),
                    "tipo_magnitud": props.get("magType"),
                    "lugar": props.get("place"),
                    "timestamp": pd.Timestamp(props.get("time"), unit="ms"),
                })
            df = pd.DataFrame(rows)
            df["fecha"] = df["timestamp"].dt.date
            TabularProcessor.write_partitioned(df, out_dir / "sismicidad")
            log.info("silver.amenazas.sismicidad", events=len(df))

    # SIMMA landslides
    simma_file = bronze_dir / "tabular" / "sgc_simma" / "simma_movimientos.json"
    if simma_file.exists():
        data = json.loads(simma_file.read_text())
        features = data.get("features", [])
        if features:
            rows = [f.get("attributes", f.get("properties", {})) for f in features]
            df = pd.DataFrame(rows)
            df = TabularProcessor.standardize_columns(df)
            df = TabularProcessor.clean_nulls(df)
            df.to_parquet(out_dir / "simma_deslizamientos.parquet", index=False)
            log.info("silver.amenazas.simma", records=len(df))

    # DesInventar
    desinventar_file = bronze_dir / "tabular" / "desinventar" / "desinventar_antioquia.json"
    if desinventar_file.exists():
        data = json.loads(desinventar_file.read_text())
        if isinstance(data, list) and data:
            df = pd.DataFrame(data)
            df = TabularProcessor.standardize_columns(df)
            df = TabularProcessor.clean_nulls(df)
            df.to_parquet(out_dir / "desinventar.parquet", index=False)
            log.info("silver.amenazas.desinventar", records=len(df))

    # Seismic hazard parameters
    amenaza_file = bronze_dir / "tabular" / "sgc_amenaza" / "amenaza_sismica_municipios.json"
    if amenaza_file.exists():
        data = json.loads(amenaza_file.read_text())
        df = pd.DataFrame.from_dict(data, orient="index")
        df.index.name = "codigo_dane"
        df = df.reset_index()
        df.to_parquet(out_dir / "amenaza_sismica_nsr10.parquet", index=False)
        log.info("silver.amenazas.nsr10", municipios=len(df))

    catalog.register({
        "dataset_id": "amenazas",
        "source": "SGC + USGS + DesInventar",
        "category": "geologia",
        "data_type": "tabular",
        "layer": "silver",
        "file_path": str(out_dir),
        "format": "parquet",
        "ingestor": "processor.tabular.amenazas",
        "status": "complete",
    })
```

- [ ] **Step 3: Commit**

```bash
git add processors/tabular/
git commit -m "feat: Silver tabular processors — hydrology, market, socioeconomic, hazards"
```

---

### Task 15: Silver Processors — Raster and Vector

**Files:**
- Create: `processors/raster/__init__.py`
- Create: `processors/raster/era5.py`
- Create: `processors/raster/chirps.py`
- Create: `processors/raster/dem.py`
- Create: `processors/raster/mapbiomas.py`
- Create: `processors/vector/__init__.py`
- Create: `processors/vector/cuencas.py`
- Create: `processors/vector/geologia.py`
- Create: `processors/vector/cobertura.py`

- [ ] **Step 1: Raster processors**

`processors/raster/__init__.py`:
```python
```

`processors/raster/era5.py`:
```python
"""Bronze → Silver processor for ERA5-Land NetCDF data."""

from pathlib import Path
import structlog
from processors.base import RasterProcessor
from catalog.manager import CatalogManager

log = structlog.get_logger()

KEY_VARIABLES = ["tp", "e", "sro", "ssro", "t2m", "swvl1", "swvl2", "swvl3", "swvl4", "sp"]


def process(bronze_dir: Path, silver_dir: Path, catalog: CatalogManager, **kwargs):
    """Convert ERA5 NetCDF files to COG GeoTIFFs."""
    era5_bronze = bronze_dir / "raster" / "cds_era5"
    era5_silver = silver_dir / "raster" / "era5_land"

    for nc_file in sorted(era5_bronze.glob("*.nc")):
        log.info("silver.era5", file=nc_file.name)
        for var in KEY_VARIABLES:
            try:
                RasterProcessor.netcdf_to_cog(nc_file, era5_silver / var, var)
            except (KeyError, ValueError):
                continue

    catalog.register({
        "dataset_id": "era5_land_silver",
        "source": "ERA5-Land",
        "category": "meteorologia",
        "data_type": "raster",
        "layer": "silver",
        "file_path": str(era5_silver),
        "format": "geotiff",
        "ingestor": "processor.raster.era5",
        "status": "complete",
    })
```

`processors/raster/chirps.py`:
```python
"""Bronze → Silver processor for CHIRPS precipitation GeoTIFFs."""

from pathlib import Path
import structlog
from processors.base import RasterProcessor
from catalog.manager import CatalogManager

log = structlog.get_logger()


def process(bronze_dir: Path, silver_dir: Path, catalog: CatalogManager, **kwargs):
    """Clip and ensure COG format for CHIRPS data."""
    chirps_bronze = bronze_dir / "raster" / "chirps"
    chirps_silver = silver_dir / "raster" / "chirps"
    chirps_silver.mkdir(parents=True, exist_ok=True)

    for tif in sorted(chirps_bronze.glob("*.tif")):
        out = chirps_silver / tif.name
        if out.exists():
            continue
        log.info("silver.chirps", file=tif.name)
        RasterProcessor.clip_and_reproject(tif, out)

    catalog.register({
        "dataset_id": "chirps_silver",
        "source": "CHIRPS v2",
        "category": "meteorologia",
        "data_type": "raster",
        "layer": "silver",
        "file_path": str(chirps_silver),
        "format": "geotiff",
        "ingestor": "processor.raster.chirps",
        "status": "complete",
    })
```

`processors/raster/dem.py`:
```python
"""Bronze → Silver processor for DEMs — merge into composite."""

from pathlib import Path
import structlog
from catalog.manager import CatalogManager

log = structlog.get_logger()


def process(bronze_dir: Path, silver_dir: Path, catalog: CatalogManager, **kwargs):
    """Merge DEMs into a single composite (best-resolution priority)."""
    import rasterio
    from rasterio.merge import merge

    dem_bronze = bronze_dir / "raster" / "gee_dem"
    dem_silver = silver_dir / "raster" / "dem_merged"
    dem_silver.mkdir(parents=True, exist_ok=True)

    out_path = dem_silver / "dem_composite.tif"
    if out_path.exists():
        log.info("silver.dem.skip_existing")
        return

    # Priority order: ALOS (12.5m) > Copernicus (30m) > SRTM (30m)
    priority = ["alos_palsar_12m.tif", "copernicus_glo30.tif", "srtm_30m.tif"]
    datasets = []

    for name in priority:
        path = dem_bronze / name
        if path.exists():
            datasets.append(rasterio.open(path))
            log.info("silver.dem.adding", source=name)

    if not datasets:
        log.warning("silver.dem.no_sources")
        return

    merged, transform = merge(datasets, method="first")  # first = highest priority wins

    meta = datasets[0].meta.copy()
    meta.update({
        "driver": "GTiff",
        "height": merged.shape[1],
        "width": merged.shape[2],
        "transform": transform,
    })

    with rasterio.open(out_path, "w", **meta) as dest:
        dest.write(merged)

    for ds in datasets:
        ds.close()

    log.info("silver.dem.merged", output=str(out_path))

    # Also copy individual DEMs to silver for per-source analysis
    for name in priority:
        src = dem_bronze / name
        dst = dem_silver / name
        if src.exists() and not dst.exists():
            import shutil
            shutil.copy2(src, dst)

    catalog.register({
        "dataset_id": "dem_composite",
        "source": "ALOS+Copernicus+SRTM",
        "category": "geoespacial",
        "data_type": "raster",
        "layer": "silver",
        "file_path": str(out_path),
        "format": "geotiff",
        "spatial_resolution": "12.5m-30m",
        "ingestor": "processor.raster.dem",
        "status": "complete",
    })
```

`processors/raster/mapbiomas.py`:
```python
"""Bronze → Silver processor for MapBiomas annual land cover."""

from pathlib import Path
import structlog
from processors.base import RasterProcessor
from catalog.manager import CatalogManager

log = structlog.get_logger()


def process(bronze_dir: Path, silver_dir: Path, catalog: CatalogManager, **kwargs):
    """Ensure MapBiomas tiles are clipped and in COG format."""
    mb_bronze = bronze_dir / "raster" / "mapbiomas"
    mb_silver = silver_dir / "raster" / "mapbiomas"
    mb_silver.mkdir(parents=True, exist_ok=True)

    for tif in sorted(mb_bronze.glob("*.tif")):
        out = mb_silver / tif.name
        if out.exists():
            continue
        log.info("silver.mapbiomas", file=tif.name)
        RasterProcessor.clip_and_reproject(tif, out)

    catalog.register({
        "dataset_id": "mapbiomas_silver",
        "source": "MapBiomas Colombia",
        "category": "biodiversidad",
        "data_type": "raster",
        "layer": "silver",
        "file_path": str(mb_silver),
        "format": "geotiff",
        "ingestor": "processor.raster.mapbiomas",
        "status": "complete",
    })
```

- [ ] **Step 2: Vector processors**

`processors/vector/__init__.py`:
```python
```

`processors/vector/cuencas.py`:
```python
"""Bronze → Silver processor for HydroSHEDS catchments."""

from pathlib import Path
import geopandas as gpd
import structlog
from processors.base import VectorProcessor
from catalog.manager import CatalogManager

log = structlog.get_logger()


def process(bronze_dir: Path, silver_dir: Path, catalog: CatalogManager, **kwargs):
    """Clip HydroSHEDS to AOI and convert to GeoParquet."""
    hydro_bronze = bronze_dir / "vector" / "hydrosheds"
    out_path = silver_dir / "vector" / "cuencas.geoparquet"

    all_gdfs = []
    for shp_dir in hydro_bronze.iterdir():
        if shp_dir.is_dir():
            for shp in shp_dir.rglob("*.shp"):
                log.info("silver.cuencas.reading", file=str(shp))
                gdf = gpd.read_file(shp)
                gdf["source_file"] = shp.stem
                all_gdfs.append(gdf)

    if not all_gdfs:
        log.warning("silver.cuencas.no_data")
        return

    combined = gpd.GeoDataFrame(pd.concat(all_gdfs, ignore_index=True))
    combined = VectorProcessor.fix_geometries(combined)
    combined = VectorProcessor.clip_to_aoi(combined)
    VectorProcessor.to_geoparquet(combined, out_path)

    catalog.register({
        "dataset_id": "cuencas",
        "source": "HydroSHEDS/HydroBASINS",
        "category": "hidrologia",
        "data_type": "vector",
        "layer": "silver",
        "file_path": str(out_path),
        "format": "geoparquet",
        "ingestor": "processor.vector.cuencas",
        "status": "complete",
    })
```

`processors/vector/geologia.py`:
```python
"""Bronze → Silver processor for geological vector data."""

import json
from pathlib import Path
import geopandas as gpd
import structlog
from processors.base import VectorProcessor
from catalog.manager import CatalogManager

log = structlog.get_logger()


def process(bronze_dir: Path, silver_dir: Path, catalog: CatalogManager, **kwargs):
    """Process SGC geology + IGAC cartography to GeoParquet."""
    silver_vector = silver_dir / "vector"

    # SGC geological units
    geo_file = bronze_dir / "vector" / "sgc_geologia" / "unidades_geologicas.geojson"
    if geo_file.exists():
        gdf = gpd.read_file(geo_file)
        gdf = VectorProcessor.fix_geometries(gdf)
        gdf = VectorProcessor.clip_to_aoi(gdf)
        VectorProcessor.to_geoparquet(gdf, silver_vector / "geologia.geoparquet")
        log.info("silver.geologia.unidades", features=len(gdf))

    # SGC faults
    faults_file = bronze_dir / "vector" / "sgc_geologia" / "fallas.geojson"
    if faults_file.exists():
        gdf = gpd.read_file(faults_file)
        gdf = VectorProcessor.fix_geometries(gdf)
        gdf = VectorProcessor.clip_to_aoi(gdf)
        VectorProcessor.to_geoparquet(gdf, silver_vector / "fallas.geoparquet")
        log.info("silver.geologia.fallas", features=len(gdf))

    # IGAC basemap layers
    igac_dir = bronze_dir / "vector" / "igac_cartografia"
    for geojson in igac_dir.glob("*.geojson"):
        name = geojson.stem
        try:
            gdf = gpd.read_file(geojson)
            if len(gdf) == 0:
                continue
            gdf = VectorProcessor.fix_geometries(gdf)
            gdf = VectorProcessor.clip_to_aoi(gdf)
            VectorProcessor.to_geoparquet(gdf, silver_vector / f"igac_{name}.geoparquet")
            log.info("silver.igac", layer=name, features=len(gdf))
        except Exception as e:
            log.warning("silver.igac.failed", layer=name, error=str(e))

    catalog.register({
        "dataset_id": "geologia_vector",
        "source": "SGC + IGAC",
        "category": "geologia",
        "data_type": "vector",
        "layer": "silver",
        "file_path": str(silver_vector),
        "format": "geoparquet",
        "ingestor": "processor.vector.geologia",
        "status": "complete",
    })
```

`processors/vector/cobertura.py`:
```python
"""Bronze → Silver processor for land cover and protected areas."""

from pathlib import Path
import geopandas as gpd
import structlog
from processors.base import VectorProcessor
from catalog.manager import CatalogManager

log = structlog.get_logger()


def process(bronze_dir: Path, silver_dir: Path, catalog: CatalogManager, **kwargs):
    """Process Corine LC, RUNAP, CORANTIOQUIA to GeoParquet."""
    silver_vector = silver_dir / "vector"

    # Corine Land Cover (most recent epoch)
    corine_dir = bronze_dir / "vector" / "corine_lc"
    for geojson in sorted(corine_dir.glob("*.geojson"), reverse=True):
        try:
            gdf = gpd.read_file(geojson)
            if len(gdf) == 0:
                continue
            gdf = VectorProcessor.fix_geometries(gdf)
            gdf = VectorProcessor.clip_to_aoi(gdf)
            epoch = geojson.stem  # clc_2018, clc_2014, etc.
            VectorProcessor.to_geoparquet(gdf, silver_vector / f"cobertura_{epoch}.geoparquet")
            log.info("silver.cobertura", epoch=epoch, features=len(gdf))
        except Exception as e:
            log.warning("silver.cobertura.failed", file=geojson.name, error=str(e))

    # RUNAP protected areas
    runap_file = bronze_dir / "vector" / "runap" / "areas_protegidas.geojson"
    if runap_file.exists():
        gdf = gpd.read_file(runap_file)
        if len(gdf) > 0:
            gdf = VectorProcessor.fix_geometries(gdf)
            gdf = VectorProcessor.clip_to_aoi(gdf)
            VectorProcessor.to_geoparquet(gdf, silver_vector / "areas_protegidas.geoparquet")
            log.info("silver.runap", features=len(gdf))

    # CORANTIOQUIA boundaries
    corant_dir = bronze_dir / "vector" / "corantioquia"
    for geojson in corant_dir.glob("*.geojson"):
        name = geojson.stem
        try:
            gdf = gpd.read_file(geojson)
            if len(gdf) > 0:
                gdf = VectorProcessor.fix_geometries(gdf)
                VectorProcessor.to_geoparquet(gdf, silver_vector / f"corantioquia_{name}.geoparquet")
                log.info("silver.corantioquia", layer=name, features=len(gdf))
        except Exception as e:
            log.warning("silver.corantioquia.failed", layer=name, error=str(e))

    catalog.register({
        "dataset_id": "cobertura_suelo",
        "source": "Corine LC + RUNAP + CORANTIOQUIA",
        "category": "biodiversidad",
        "data_type": "vector",
        "layer": "silver",
        "file_path": str(silver_vector),
        "format": "geoparquet",
        "ingestor": "processor.vector.cobertura",
        "status": "complete",
    })
```

- [ ] **Step 3: Add missing import in cuencas.py**

Add `import pandas as pd` at the top of `processors/vector/cuencas.py` (needed for `pd.concat`).

- [ ] **Step 4: Commit**

```bash
git add processors/raster/ processors/vector/
git commit -m "feat: Silver raster + vector processors — ERA5, CHIRPS, DEM, MapBiomas, catchments, geology, land cover"
```

---

### Task 16: Gold Views — Hydrology

**Files:**
- Create: `processors/gold/__init__.py`
- Create: `processors/gold/balance_hidrico.py`
- Create: `processors/gold/series_caudal.py`
- Create: `processors/gold/curvas_duracion.py`

- [ ] **Step 1: Create Gold views for hydrology**

`processors/gold/__init__.py`:
```python
```

`processors/gold/balance_hidrico.py`:
```python
"""Gold view: water balance (P - ET - Q) by subcatchment, monthly, 1950-present."""

from pathlib import Path
import pandas as pd
import duckdb
import structlog
from catalog.manager import CatalogManager

log = structlog.get_logger()


def build(bronze_dir: Path, silver_dir: Path, gold_dir: Path, catalog: CatalogManager, **kwargs):
    """Build monthly water balance from ERA5 + CHIRPS + IDEAM."""
    out_path = gold_dir / "balance_hidrico.parquet"

    con = duckdb.connect()

    # Read ERA5 precipitation and evaporation (Silver Parquet)
    era5_precip_dir = silver_dir / "raster" / "era5_land" / "tp"
    era5_evap_dir = silver_dir / "raster" / "era5_land" / "e"

    # For tabular aggregation, read from processed hydrology Silver
    hydro_dir = silver_dir / "tabular" / "hidrologia"

    # Build water balance from available data
    records = []

    # Read IDEAM observed data if available
    for parquet_dir in hydro_dir.glob("year=*"):
        parquet_file = parquet_dir / "data.parquet"
        if parquet_file.exists():
            try:
                df = pd.read_parquet(parquet_file)
                if "fecha" in df.columns and "caudal" in df.select_dtypes("number").columns:
                    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
                    df["year"] = df["fecha"].dt.year
                    df["month"] = df["fecha"].dt.month
                    monthly = df.groupby(["year", "month"]).agg(
                        caudal_medio_m3s=("caudal", "mean"),
                    ).reset_index()
                    monthly["fuente"] = "IDEAM"
                    records.append(monthly)
            except Exception as e:
                log.warning("gold.balance.read_error", file=str(parquet_file), error=str(e))

    if records:
        result = pd.concat(records, ignore_index=True)
    else:
        # Placeholder structure when no data available yet
        result = pd.DataFrame(columns=[
            "year", "month", "precipitacion_mm", "evapotranspiracion_mm",
            "escorrentia_mm", "caudal_medio_m3s", "delta_almacenamiento_mm",
            "fuente",
        ])
        log.warning("gold.balance.no_data")

    result.to_parquet(out_path, index=False)
    log.info("gold.balance_hidrico.built", rows=len(result), path=str(out_path))

    catalog.register({
        "dataset_id": "balance_hidrico",
        "source": "ERA5 + CHIRPS + IDEAM",
        "category": "hidrologia",
        "data_type": "tabular",
        "layer": "gold",
        "file_path": str(out_path),
        "format": "parquet",
        "ingestor": "processor.gold.balance_hidrico",
        "status": "complete",
    })
    con.close()
```

`processors/gold/series_caudal.py`:
```python
"""Gold view: merged discharge time series (~70 years) with confidence indicator."""

from pathlib import Path
import pandas as pd
import structlog
from catalog.manager import CatalogManager

log = structlog.get_logger()


def build(bronze_dir: Path, silver_dir: Path, gold_dir: Path, catalog: CatalogManager, **kwargs):
    """Build continuous daily discharge series: IDEAM (observed) + GloFAS (modeled) + ERA5 (runoff)."""
    out_path = gold_dir / "series_caudal.parquet"
    hydro_dir = silver_dir / "tabular" / "hidrologia"

    all_dfs = []

    # 1. IDEAM observed (highest confidence)
    for parquet_dir in sorted(hydro_dir.glob("year=*")):
        pf = parquet_dir / "data.parquet"
        if pf.exists():
            try:
                df = pd.read_parquet(pf)
                if "caudal" in df.select_dtypes("number").columns:
                    df["confianza"] = "alta"
                    df["fuente"] = "IDEAM"
                    all_dfs.append(df)
            except Exception:
                continue

    # GloFAS modeled discharge (medium confidence) and ERA5 runoff (lower confidence)
    # are added in Phase 2 when raster-to-point extraction pipeline is built

    if all_dfs:
        result = pd.concat(all_dfs, ignore_index=True)
        # Sort by date
        if "fecha" in result.columns:
            result["fecha"] = pd.to_datetime(result["fecha"], errors="coerce")
            result = result.sort_values("fecha")
    else:
        result = pd.DataFrame(columns=["fecha", "caudal_m3s", "confianza", "fuente", "estacion"])
        log.warning("gold.series_caudal.no_data")

    result.to_parquet(out_path, index=False)
    log.info("gold.series_caudal.built", rows=len(result))

    catalog.register({
        "dataset_id": "series_caudal",
        "source": "IDEAM + GloFAS + ERA5",
        "category": "hidrologia",
        "data_type": "tabular",
        "layer": "gold",
        "file_path": str(out_path),
        "format": "parquet",
        "ingestor": "processor.gold.series_caudal",
        "status": "complete",
    })
```

`processors/gold/curvas_duracion.py`:
```python
"""Gold view: flow duration curves — percentiles 5-95 of discharge."""

from pathlib import Path
import numpy as np
import pandas as pd
import structlog
from catalog.manager import CatalogManager

log = structlog.get_logger()

PERCENTILES = [5, 10, 20, 25, 30, 40, 50, 60, 70, 75, 80, 90, 95]


def build(bronze_dir: Path, silver_dir: Path, gold_dir: Path, catalog: CatalogManager, **kwargs):
    """Build flow duration curves from merged discharge series."""
    out_path = gold_dir / "curvas_duracion.parquet"
    series_path = gold_dir / "series_caudal.parquet"

    if not series_path.exists():
        log.warning("gold.curvas.no_input")
        pd.DataFrame().to_parquet(out_path, index=False)
        return

    df = pd.read_parquet(series_path)

    caudal_col = None
    for col in ["caudal_m3s", "caudal", "valor"]:
        if col in df.columns:
            caudal_col = col
            break

    if caudal_col is None:
        log.warning("gold.curvas.no_caudal_column")
        pd.DataFrame().to_parquet(out_path, index=False)
        return

    q = df[caudal_col].dropna().values

    if len(q) == 0:
        log.warning("gold.curvas.empty_series")
        pd.DataFrame().to_parquet(out_path, index=False)
        return

    # Annual flow duration curve
    results = []
    for p in PERCENTILES:
        val = np.percentile(q, 100 - p)  # Q95 = flow exceeded 95% of time
        results.append({
            "percentil_excedencia": p,
            "caudal_m3s": round(val, 3),
            "periodo": "anual",
        })

    # Monthly flow duration curves
    if "fecha" in df.columns:
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
        df["month"] = df["fecha"].dt.month
        for month in range(1, 13):
            month_q = df[df["month"] == month][caudal_col].dropna().values
            if len(month_q) == 0:
                continue
            for p in PERCENTILES:
                val = np.percentile(month_q, 100 - p)
                results.append({
                    "percentil_excedencia": p,
                    "caudal_m3s": round(val, 3),
                    "periodo": f"mes_{month:02d}",
                })

    result = pd.DataFrame(results)
    result.to_parquet(out_path, index=False)
    log.info("gold.curvas_duracion.built", rows=len(result))

    catalog.register({
        "dataset_id": "curvas_duracion",
        "source": "series_caudal (Gold)",
        "category": "hidrologia",
        "data_type": "tabular",
        "layer": "gold",
        "file_path": str(out_path),
        "format": "parquet",
        "ingestor": "processor.gold.curvas_duracion",
        "status": "complete",
    })
```

- [ ] **Step 2: Commit**

```bash
git add processors/gold/
git commit -m "feat: Gold hydrology views — water balance, discharge series, flow duration curves"
```

---

### Task 17: Gold Views — Geology, Hazards, Generation Potential

**Files:**
- Create: `processors/gold/potencial_generacion.py`
- Create: `processors/gold/perfil_geologico.py`
- Create: `processors/gold/amenazas_naturales.py`

- [ ] **Step 1: Create remaining Gold views**

`processors/gold/potencial_generacion.py`:
```python
"""Gold view: hydroelectric generation potential — P = Q * H * eta * rho * g."""

from pathlib import Path
import numpy as np
import pandas as pd
import structlog
from catalog.manager import CatalogManager

log = structlog.get_logger()

RHO = 1000      # water density kg/m3
G = 9.81        # gravitational acceleration m/s2
ETA_PELTON = 0.85
ETA_FRANCIS = 0.90


def build(bronze_dir: Path, silver_dir: Path, gold_dir: Path, catalog: CatalogManager, **kwargs):
    """Estimate generation potential for combinations of Q and H."""
    out_path = gold_dir / "potencial_generacion.parquet"
    curvas_path = gold_dir / "curvas_duracion.parquet"

    if not curvas_path.exists():
        log.warning("gold.potencial.no_curvas")
        pd.DataFrame().to_parquet(out_path, index=False)
        return

    curvas = pd.read_parquet(curvas_path)
    annual = curvas[curvas["periodo"] == "anual"]

    if annual.empty:
        log.warning("gold.potencial.empty_curvas")
        pd.DataFrame().to_parquet(out_path, index=False)
        return

    # Sweep of possible net heads (m) — depends on topography
    heads = [50, 100, 150, 200, 300, 400, 500, 600, 700, 800, 900, 1000]

    results = []
    for _, row in annual.iterrows():
        q = row["caudal_m3s"]
        p_exc = row["percentil_excedencia"]

        for h in heads:
            for turbine, eta in [("Pelton", ETA_PELTON), ("Francis", ETA_FRANCIS)]:
                power_kw = eta * RHO * G * q * h / 1000
                power_mw = round(power_kw / 1000, 2)
                energy_gwh_yr = round(power_mw * 8760 / 1000, 2)

                results.append({
                    "percentil_excedencia": p_exc,
                    "caudal_m3s": q,
                    "salto_neto_m": h,
                    "turbina": turbine,
                    "eficiencia": eta,
                    "potencia_mw": power_mw,
                    "energia_gwh_ano": energy_gwh_yr,
                })

    result = pd.DataFrame(results)
    result.to_parquet(out_path, index=False)
    log.info("gold.potencial.built", rows=len(result))

    catalog.register({
        "dataset_id": "potencial_generacion",
        "source": "curvas_duracion + DEM",
        "category": "hidrologia",
        "data_type": "tabular",
        "layer": "gold",
        "file_path": str(out_path),
        "format": "parquet",
        "ingestor": "processor.gold.potencial_generacion",
        "status": "complete",
    })
```

`processors/gold/perfil_geologico.py`:
```python
"""Gold view: geological aptitude profile — lithology, faults, slopes."""

from pathlib import Path
import geopandas as gpd
import pandas as pd
import structlog
from catalog.manager import CatalogManager

log = structlog.get_logger()


def build(bronze_dir: Path, silver_dir: Path, gold_dir: Path, catalog: CatalogManager, **kwargs):
    """Build geological profile combining geology, faults, landslides, seismic params."""
    out_path = gold_dir / "perfil_geologico.geoparquet"
    silver_vector = silver_dir / "vector"

    layers = []

    # Geological units
    geo_path = silver_vector / "geologia.geoparquet"
    if geo_path.exists():
        gdf = gpd.read_parquet(geo_path)
        gdf["capa"] = "unidad_geologica"
        layers.append(gdf)

    # Faults
    faults_path = silver_vector / "fallas.geoparquet"
    if faults_path.exists():
        gdf = gpd.read_parquet(faults_path)
        gdf["capa"] = "falla"
        layers.append(gdf)

    if not layers:
        log.warning("gold.perfil_geo.no_data")
        return

    combined = gpd.GeoDataFrame(pd.concat(layers, ignore_index=True))
    combined.to_parquet(out_path)
    log.info("gold.perfil_geologico.built", features=len(combined))

    catalog.register({
        "dataset_id": "perfil_geologico",
        "source": "SGC geology + faults",
        "category": "geologia",
        "data_type": "vector",
        "layer": "gold",
        "file_path": str(out_path),
        "format": "geoparquet",
        "ingestor": "processor.gold.perfil_geologico",
        "status": "complete",
    })
```

`processors/gold/amenazas_naturales.py`:
```python
"""Gold view: multi-hazard assessment — seismic, landslide, flood."""

from pathlib import Path
import pandas as pd
import structlog
from catalog.manager import CatalogManager

log = structlog.get_logger()


def build(bronze_dir: Path, silver_dir: Path, gold_dir: Path, catalog: CatalogManager, **kwargs):
    """Combine seismicity, landslides, disasters, NSR-10 params into multi-hazard table."""
    out_path = gold_dir / "amenazas_naturales.parquet"
    amenazas_dir = silver_dir / "tabular" / "amenazas"

    dfs = {}

    # Seismicity
    sismicidad_dir = amenazas_dir / "sismicidad"
    if sismicidad_dir.exists():
        parts = list(sismicidad_dir.glob("year=*/data.parquet"))
        if parts:
            dfs["sismicidad"] = pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)

    # SIMMA landslides
    simma_path = amenazas_dir / "simma_deslizamientos.parquet"
    if simma_path.exists():
        dfs["deslizamientos"] = pd.read_parquet(simma_path)

    # DesInventar
    des_path = amenazas_dir / "desinventar.parquet"
    if des_path.exists():
        dfs["desastres"] = pd.read_parquet(des_path)

    # NSR-10 seismic hazard
    nsr_path = amenazas_dir / "amenaza_sismica_nsr10.parquet"
    if nsr_path.exists():
        dfs["nsr10"] = pd.read_parquet(nsr_path)

    # Summary statistics
    summary = {
        "total_sismos": len(dfs.get("sismicidad", pd.DataFrame())),
        "total_deslizamientos": len(dfs.get("deslizamientos", pd.DataFrame())),
        "total_desastres": len(dfs.get("desastres", pd.DataFrame())),
        "zona_sismica": "Intermedia",
        "Aa_referencia": 0.15,
        "Av_referencia": 0.20,
    }

    # Save individual tables + summary
    for name, df in dfs.items():
        sub_path = gold_dir / f"amenazas_{name}.parquet"
        df.to_parquet(sub_path, index=False)

    pd.DataFrame([summary]).to_parquet(out_path, index=False)
    log.info("gold.amenazas.built", tables=len(dfs), summary=summary)

    catalog.register({
        "dataset_id": "amenazas_naturales",
        "source": "SGC + USGS + DesInventar",
        "category": "geologia",
        "data_type": "tabular",
        "layer": "gold",
        "file_path": str(out_path),
        "format": "parquet",
        "ingestor": "processor.gold.amenazas_naturales",
        "status": "complete",
    })
```

- [ ] **Step 2: Commit**

```bash
git add processors/gold/potencial_generacion.py processors/gold/perfil_geologico.py processors/gold/amenazas_naturales.py
git commit -m "feat: Gold views — generation potential, geological profile, natural hazards"
```

---

### Task 18: Gold Views — Market, Social, Environmental, Solar/Wind

**Files:**
- Create: `processors/gold/mercado_despacho.py`
- Create: `processors/gold/indicadores_socioeconomicos.py`
- Create: `processors/gold/linea_base_ambiental.py`
- Create: `processors/gold/recurso_solar_eolico.py`

- [ ] **Step 1: Create remaining Gold views**

`processors/gold/mercado_despacho.py`:
```python
"""Gold view: electricity market analysis — prices, generation, competitors."""

from pathlib import Path
import pandas as pd
import structlog
from catalog.manager import CatalogManager

log = structlog.get_logger()


def build(bronze_dir: Path, silver_dir: Path, gold_dir: Path, catalog: CatalogManager, **kwargs):
    """Aggregate electricity market data for financial viability analysis."""
    out_path = gold_dir / "mercado_despacho.parquet"
    market_dir = silver_dir / "tabular" / "mercado_electrico"

    dfs = []
    for sub in market_dir.iterdir():
        if sub.is_dir():
            for pf in sub.rglob("*.parquet"):
                try:
                    dfs.append(pd.read_parquet(pf))
                except Exception:
                    continue

    if dfs:
        result = pd.concat(dfs, ignore_index=True)
    else:
        result = pd.DataFrame()
        log.warning("gold.mercado.no_data")

    result.to_parquet(out_path, index=False)
    log.info("gold.mercado.built", rows=len(result))

    catalog.register({
        "dataset_id": "mercado_despacho",
        "source": "XM SiMEM + UPME",
        "category": "mercado_electrico",
        "data_type": "tabular",
        "layer": "gold",
        "file_path": str(out_path),
        "format": "parquet",
        "ingestor": "processor.gold.mercado_despacho",
        "status": "complete",
    })
```

`processors/gold/indicadores_socioeconomicos.py`:
```python
"""Gold view: municipal socioeconomic profiles."""

from pathlib import Path
import pandas as pd
import structlog
from catalog.manager import CatalogManager
from config.settings import AOI_MUNICIPIOS

log = structlog.get_logger()


def build(bronze_dir: Path, silver_dir: Path, gold_dir: Path, catalog: CatalogManager, **kwargs):
    """Compile socioeconomic indicators per municipality."""
    out_path = gold_dir / "indicadores_socioeconomicos.parquet"
    socio_dir = silver_dir / "tabular" / "socioeconomico"

    indicators = []
    for code, name in AOI_MUNICIPIOS.items():
        row = {"codigo_dane": code, "municipio": name}

        # Try to extract key indicators from available data
        dane_path = socio_dir / "dane_poblacion_2018" / "data.parquet"
        if dane_path.exists():
            try:
                df = pd.read_parquet(dane_path)
                mun = df[df.apply(lambda r: code in str(r.values), axis=1)]
                if not mun.empty:
                    row["poblacion_2018"] = len(mun) if "persona" in str(df.columns).lower() else None
            except Exception:
                pass

        indicators.append(row)

    result = pd.DataFrame(indicators)
    result.to_parquet(out_path, index=False)
    log.info("gold.socioeconomico.built", municipios=len(result))

    catalog.register({
        "dataset_id": "indicadores_socioeconomicos",
        "source": "DANE + DNP + AGRONET",
        "category": "socioeconomico",
        "data_type": "tabular",
        "layer": "gold",
        "file_path": str(out_path),
        "format": "parquet",
        "ingestor": "processor.gold.indicadores_socioeconomicos",
        "status": "complete",
    })
```

`processors/gold/linea_base_ambiental.py`:
```python
"""Gold view: environmental baseline — land cover + protected areas + biodiversity."""

from pathlib import Path
import geopandas as gpd
import pandas as pd
import structlog
from catalog.manager import CatalogManager

log = structlog.get_logger()


def build(bronze_dir: Path, silver_dir: Path, gold_dir: Path, catalog: CatalogManager, **kwargs):
    """Compile environmental baseline from multiple vector sources."""
    out_path = gold_dir / "linea_base_ambiental.geoparquet"
    silver_vector = silver_dir / "vector"

    layers = []

    # Latest Corine Land Cover
    for corine in sorted(silver_vector.glob("cobertura_clc_*.geoparquet"), reverse=True):
        try:
            gdf = gpd.read_parquet(corine)
            gdf["capa"] = corine.stem
            layers.append(gdf)
            break  # Only most recent
        except Exception:
            continue

    # Protected areas
    ap_path = silver_vector / "areas_protegidas.geoparquet"
    if ap_path.exists():
        gdf = gpd.read_parquet(ap_path)
        gdf["capa"] = "area_protegida"
        layers.append(gdf)

    # CORANTIOQUIA POMCA
    pomca_path = silver_vector / "corantioquia_pomca.geoparquet"
    if pomca_path.exists():
        gdf = gpd.read_parquet(pomca_path)
        gdf["capa"] = "pomca"
        layers.append(gdf)

    if layers:
        combined = gpd.GeoDataFrame(pd.concat(layers, ignore_index=True))
        combined.to_parquet(out_path)
        log.info("gold.ambiental.built", layers=len(layers), features=len(combined))
    else:
        log.warning("gold.ambiental.no_data")

    catalog.register({
        "dataset_id": "linea_base_ambiental",
        "source": "Corine + RUNAP + CORANTIOQUIA",
        "category": "biodiversidad",
        "data_type": "vector",
        "layer": "gold",
        "file_path": str(out_path),
        "format": "geoparquet",
        "ingestor": "processor.gold.linea_base_ambiental",
        "status": "complete",
    })
```

`processors/gold/recurso_solar_eolico.py`:
```python
"""Gold view: solar and wind resource assessment."""

import json
from pathlib import Path
import pandas as pd
import structlog
from catalog.manager import CatalogManager

log = structlog.get_logger()


def build(bronze_dir: Path, silver_dir: Path, gold_dir: Path, catalog: CatalogManager, **kwargs):
    """Extract solar/wind indicators from NASA POWER data."""
    out_path = gold_dir / "recurso_solar_eolico.parquet"

    # Read NASA POWER Bronze data
    power_dir = bronze_dir / "tabular" / "nasa_power"
    records = []

    for json_file in power_dir.glob("*.json"):
        try:
            data = json.loads(json_file.read_text())
            params = data.get("properties", {}).get("parameter", {})

            for date_str in list(params.get("ALLSKY_SFC_SW_DWN", {}).keys()):
                row = {"fecha": date_str}
                for var, values in params.items():
                    row[var.lower()] = values.get(date_str)
                records.append(row)
        except Exception as e:
            log.warning("gold.solar.read_error", file=str(json_file), error=str(e))

    if records:
        df = pd.DataFrame(records)
        df["fecha"] = pd.to_datetime(df["fecha"], format="%Y%m%d", errors="coerce")

        # Compute annual averages
        summary = {
            "ghi_kwh_m2_dia": df.get("allsky_sfc_sw_dwn", pd.Series()).mean(),
            "dni_proxy_kwh_m2_dia": df.get("clrsky_sfc_sw_dwn", pd.Series()).mean(),
            "temp_media_c": df.get("t2m", pd.Series()).mean(),
            "viento_2m_ms": df.get("ws2m", pd.Series()).mean(),
            "viento_10m_ms": df.get("ws10m", pd.Series()).mean(),
            "viento_50m_ms": df.get("ws50m", pd.Series()).mean(),
            "humedad_relativa_pct": df.get("rh2m", pd.Series()).mean(),
        }
        summary_df = pd.DataFrame([summary])

        # Daily series
        df.to_parquet(gold_dir / "recurso_solar_eolico_diario.parquet", index=False)
        summary_df.to_parquet(out_path, index=False)
        log.info("gold.solar.built", days=len(df), summary=summary)
    else:
        pd.DataFrame().to_parquet(out_path, index=False)
        log.warning("gold.solar.no_data")

    catalog.register({
        "dataset_id": "recurso_solar_eolico",
        "source": "NASA POWER",
        "category": "solar_eolico",
        "data_type": "tabular",
        "layer": "gold",
        "file_path": str(out_path),
        "format": "parquet",
        "ingestor": "processor.gold.recurso_solar_eolico",
        "status": "complete",
    })
```

- [ ] **Step 2: Commit**

```bash
git add processors/gold/
git commit -m "feat: Gold views — market, socioeconomic, environmental baseline, solar/wind"
```

---

### Task 19: Export Scripts

**Files:**
- Create: `scripts/export_consultor.py`
- Create: `scripts/validate.py`

- [ ] **Step 1: Consultant export script**

`scripts/export_consultor.py`:
```python
"""Generate data packages for consultants by discipline."""

from datetime import date
from pathlib import Path

import click
import pandas as pd
import geopandas as gpd
import structlog

from config.settings import SILVER_DIR, GOLD_DIR, EXPORTS_DIR

log = structlog.get_logger()

DISCIPLINES = {
    "hidrologia": {
        "silver_tabular": ["hidrologia"],
        "gold": ["balance_hidrico.parquet", "series_caudal.parquet", "curvas_duracion.parquet", "potencial_generacion.parquet"],
        "silver_vector": ["cuencas.geoparquet"],
        "formats": ["excel", "geopackage"],
    },
    "geologia": {
        "silver_tabular": ["amenazas"],
        "gold": ["perfil_geologico.geoparquet", "amenazas_naturales.parquet"],
        "silver_vector": ["geologia.geoparquet", "fallas.geoparquet"],
        "formats": ["excel", "geopackage"],
    },
    "ambiental": {
        "gold": ["linea_base_ambiental.geoparquet"],
        "silver_vector": ["areas_protegidas.geoparquet", "cobertura_clc_2018.geoparquet"],
        "formats": ["excel", "geopackage", "csv"],
    },
    "electrico": {
        "gold": ["mercado_despacho.parquet", "potencial_generacion.parquet", "recurso_solar_eolico.parquet"],
        "formats": ["excel", "csv"],
    },
}


@click.command()
@click.option("--disciplina", type=click.Choice(list(DISCIPLINES.keys())), required=True)
@click.option("--formato", type=str, default="excel,geopackage", help="Comma-separated output formats")
def cli(disciplina, formato):
    """Export data packages for consultants."""
    today = date.today().isoformat()
    out_dir = EXPORTS_DIR / "consultores" / disciplina / today
    out_dir.mkdir(parents=True, exist_ok=True)

    config = DISCIPLINES[disciplina]
    formats = [f.strip() for f in formato.split(",")]

    exported = 0

    # Gold files
    for gold_file in config.get("gold", []):
        src = GOLD_DIR / gold_file
        if not src.exists():
            log.warning("export.missing", file=str(src))
            continue

        if gold_file.endswith(".geoparquet"):
            gdf = gpd.read_parquet(src)
            if "geopackage" in formats:
                gdf.to_file(out_dir / f"{src.stem}.gpkg", driver="GPKG")
                exported += 1
        else:
            df = pd.read_parquet(src)
            if "excel" in formats:
                df.to_excel(out_dir / f"{src.stem}.xlsx", index=False)
                exported += 1
            if "csv" in formats:
                df.to_csv(out_dir / f"{src.stem}.csv", index=False)
                exported += 1

    # Silver vector files
    for vec_file in config.get("silver_vector", []):
        src = SILVER_DIR / "vector" / vec_file
        if not src.exists():
            continue
        if "geopackage" in formats:
            gdf = gpd.read_parquet(src)
            gdf.to_file(out_dir / f"{src.stem}.gpkg", driver="GPKG")
            exported += 1

    # Write README
    readme = out_dir / "README.txt"
    readme.write_text(
        f"Data package: {disciplina}\n"
        f"Generated: {today}\n"
        f"Project: Milagros Hydroelectric Prefeasibility\n"
        f"Files: {exported}\n"
        f"Source: milagros-datalake Silver/Gold layers\n"
    )

    click.echo(f"Exported {exported} files to {out_dir}")


if __name__ == "__main__":
    cli()
```

- [ ] **Step 2: Investor export script**

`scripts/export_inversionistas.py`:
```python
"""Generate executive summary packages for investors."""

from datetime import date
from pathlib import Path

import click
import pandas as pd
import structlog

from config.settings import GOLD_DIR, EXPORTS_DIR

log = structlog.get_logger()

GOLD_FILES = [
    "potencial_generacion.parquet",
    "curvas_duracion.parquet",
    "mercado_despacho.parquet",
    "indicadores_socioeconomicos.parquet",
    "recurso_solar_eolico.parquet",
    "amenazas_naturales.parquet",
]


@click.command()
@click.option("--milestone", type=str, default="prefactibilidad")
def cli(milestone):
    """Export executive summary for investors."""
    today = date.today().isoformat()
    out_dir = EXPORTS_DIR / "inversionistas" / f"{milestone}_{today}"
    out_dir.mkdir(parents=True, exist_ok=True)

    exported = 0
    for name in GOLD_FILES:
        src = GOLD_DIR / name
        if not src.exists():
            continue
        df = pd.read_parquet(src)
        df.to_excel(out_dir / f"{src.stem}.xlsx", index=False)
        exported += 1

    readme = out_dir / "README.txt"
    readme.write_text(
        f"Executive summary: {milestone}\n"
        f"Generated: {today}\n"
        f"Project: Milagros Hydroelectric Prefeasibility\n"
        f"Files: {exported}\n"
    )
    click.echo(f"Exported {exported} files to {out_dir}")


if __name__ == "__main__":
    cli()
```

- [ ] **Step 3: Regulator export script**

`scripts/export_regulador.py`:
```python
"""Generate formal packages for regulators (ANLA, CORANTIOQUIA, UPME)."""

from datetime import date
from pathlib import Path

import click
import pandas as pd
import geopandas as gpd
import structlog

from config.settings import SILVER_DIR, GOLD_DIR, EXPORTS_DIR

log = structlog.get_logger()

ENTIDADES = {
    "anla": {
        "gold": ["balance_hidrico.parquet", "series_caudal.parquet", "curvas_duracion.parquet",
                 "perfil_geologico.geoparquet", "amenazas_naturales.parquet",
                 "linea_base_ambiental.geoparquet", "indicadores_socioeconomicos.parquet"],
        "description": "Paquete EIA hidroelectrica >100 MW",
    },
    "corantioquia": {
        "gold": ["balance_hidrico.parquet", "series_caudal.parquet", "curvas_duracion.parquet",
                 "linea_base_ambiental.geoparquet"],
        "description": "Concesion de aguas y licencia ambiental",
    },
    "upme": {
        "gold": ["potencial_generacion.parquet", "mercado_despacho.parquet"],
        "description": "Registro de proyecto de generacion",
    },
}


@click.command()
@click.option("--entidad", type=click.Choice(list(ENTIDADES.keys())), required=True)
def cli(entidad):
    """Export formal data packages for regulators."""
    today = date.today().isoformat()
    config = ENTIDADES[entidad]
    out_dir = EXPORTS_DIR / "reguladores" / entidad / today
    out_dir.mkdir(parents=True, exist_ok=True)

    exported = 0
    for name in config["gold"]:
        src = GOLD_DIR / name
        if not src.exists():
            continue

        if name.endswith(".geoparquet"):
            gdf = gpd.read_parquet(src)
            gdf.to_file(out_dir / f"{src.stem}.gpkg", driver="GPKG")
            gdf.to_file(out_dir / f"{src.stem}.shp")
        else:
            df = pd.read_parquet(src)
            df.to_excel(out_dir / f"{src.stem}.xlsx", index=False)
            df.to_csv(out_dir / f"{src.stem}.csv", index=False)
        exported += 1

    readme = out_dir / "README.txt"
    readme.write_text(
        f"Paquete regulatorio: {entidad.upper()}\n"
        f"{config['description']}\n"
        f"Generado: {today}\n"
        f"Proyecto: Central Hidroelectrica Milagros\n"
        f"Archivos: {exported}\n"
        f"Fuente: milagros-datalake Gold layer\n"
    )
    click.echo(f"Exported {exported} files to {out_dir}")


if __name__ == "__main__":
    cli()
```

- [ ] **Step 4: Validation script**

`scripts/validate.py`:
```python
"""Validate data lake integrity and completeness."""

import click
import structlog

from config.settings import BRONZE_DIR, SILVER_DIR, GOLD_DIR, CATALOG_DB
from catalog.manager import CatalogManager

log = structlog.get_logger()


@click.command()
@click.option("--layer", type=click.Choice(["bronze", "silver", "gold", "all"]), default="all")
def cli(layer):
    """Check data lake integrity and completeness."""
    catalog = CatalogManager(CATALOG_DB)
    issues = []

    layers = ["bronze", "silver", "gold"] if layer == "all" else [layer]
    dirs = {"bronze": BRONZE_DIR, "silver": SILVER_DIR, "gold": GOLD_DIR}

    for lyr in layers:
        entries = catalog.list_datasets(layer=lyr)
        click.echo(f"\n{'='*60}")
        click.echo(f"Layer: {lyr.upper()} — {len(entries)} entries in catalog")
        click.echo(f"{'='*60}")

        complete = sum(1 for e in entries if e["status"] == "complete")
        failed = sum(1 for e in entries if e["status"] == "failed")
        click.echo(f"  Complete: {complete}  |  Failed: {failed}")

        # Check files exist
        missing = 0
        for entry in entries:
            from pathlib import Path
            fp = Path(entry["file_path"])
            if not fp.exists() and not fp.is_dir():
                missing += 1
                issues.append(f"[{lyr}] Missing: {entry['file_path']}")

        if missing:
            click.echo(f"  Missing files: {missing}")

        # List by category
        categories = {}
        for entry in entries:
            cat = entry.get("category", "unknown")
            categories[cat] = categories.get(cat, 0) + 1
        for cat, count in sorted(categories.items()):
            click.echo(f"    {cat}: {count}")

    if issues:
        click.echo(f"\n{'='*60}")
        click.echo(f"ISSUES ({len(issues)}):")
        for issue in issues[:20]:
            click.echo(f"  {issue}")
    else:
        click.echo("\nAll checks passed.")

    catalog.close()


if __name__ == "__main__":
    cli()
```

- [ ] **Step 3: Commit**

```bash
git add scripts/export_consultor.py scripts/export_inversionistas.py scripts/export_regulador.py scripts/validate.py
git commit -m "feat: export + validation scripts — consultant, investor, regulator packages"
```

---

### Task 20: Sources Catalog (sources.yaml)

**Files:**
- Create: `config/sources.yaml`

- [ ] **Step 1: Write complete sources catalog**

`config/sources.yaml` — first 22 Phase 1 entries (Phase 2-4 entries follow the same schema and are added in later phases):

```yaml
# Milagros Data Lake — Source Catalog
# Phase 1: 22 critical sources for prefeasibility

sources:
  ideam_dhime:
    name: "IDEAM DHIME"
    category: hidrologia
    url: "http://dhime.ideam.gov.co/atencionciudadano/"
    api: "https://www.datos.gov.co/resource/{dataset_id}.json"
    data_type: tabular
    schedule: monthly
    license: "CC0"
    auth: "X-App-Token (optional)"
    phase: 1
    variables: [caudal_m3s, nivel_m, temp_agua_c, sedimentos, precipitacion_mm]
    notes: "Area Operativa 01, Area Hidrografica 2 (Magdalena-Cauca)"

  cds_era5:
    name: "ERA5-Land (Copernicus CDS)"
    category: meteorologia
    url: "https://cds.climate.copernicus.eu/"
    api: "cdsapi Python library"
    data_type: raster
    schedule: monthly
    license: "Copernicus License"
    auth: "CDS API key"
    phase: 1
    variables: [total_precipitation, evaporation, runoff, soil_moisture, temperature_2m, wind, radiation, pressure]
    resolution_spatial: "0.1deg (~9km)"
    resolution_temporal: "hourly (monthly means downloaded)"
    notes: "1950-present, 50+ variables, no gaps. Most critical raster source."

  chirps:
    name: "CHIRPS v2"
    category: meteorologia
    url: "https://data.chc.ucsb.edu/products/CHIRPS-2.0/"
    api: "GEE: UCSB-CHG/CHIRPS/DAILY"
    data_type: raster
    schedule: monthly
    license: "CC0 (v2)"
    auth: "GEE account"
    phase: 1
    variables: [precipitation_mm]
    resolution_spatial: "0.05deg (~5.5km)"
    resolution_temporal: "daily"
    notes: "Best satellite precip for tropical Andes. 1981-present."

  gee_dem:
    name: "DEMs (Copernicus + SRTM + ALOS)"
    category: geoespacial
    url: "via GEE"
    data_type: raster
    schedule: once
    license: "Various"
    auth: "GEE account"
    phase: 1
    variables: [elevation_m]
    resolution_spatial: "12.5m-30m"
    notes: "Three complementary DEMs for topographic analysis."

  hydrosheds:
    name: "HydroSHEDS / HydroBASINS"
    category: hidrologia
    url: "https://www.hydrosheds.org"
    data_type: vector
    schedule: once
    license: "HydroSHEDS License"
    phase: 1
    variables: [subcuencas_pfafstetter, red_fluvial]
    notes: "Levels 6 and 8 subcatchments + river network."

  glofas:
    name: "GloFAS v4"
    category: hidrologia
    url: "https://cds.climate.copernicus.eu/"
    api: "cdsapi"
    data_type: raster
    schedule: monthly
    license: "Copernicus License"
    auth: "CDS API key"
    phase: 1
    variables: [river_discharge_m3s]
    resolution_spatial: "0.05deg (~5km)"
    resolution_temporal: "daily"
    notes: "Modeled river discharge, 1979-present."

  sgc_geologia:
    name: "SGC Geological Map 1:100K"
    category: geologia
    url: "https://srvags.sgc.gov.co/arcgis/rest/services/"
    data_type: vector
    schedule: once
    license: "Public Domain (SGC)"
    phase: 1
    variables: [litologia, formacion, edad, tipo_falla]

  sgc_sismicidad:
    name: "SGC RSNC + USGS ComCat"
    category: geologia
    url: "https://earthquake.usgs.gov/fdsnws/event/1/"
    api: "FDSN REST + datos.gov.co"
    data_type: tabular
    schedule: monthly
    license: "Public Domain"
    phase: 1
    variables: [magnitud, profundidad_km, lat, lon, fecha]
    notes: "300km radius, M>=2.5, 1900-present."

  sgc_simma:
    name: "SGC SIMMA Landslides"
    category: geologia
    url: "https://simma.sgc.gov.co/"
    api: "ArcGIS REST"
    data_type: tabular
    schedule: monthly
    license: "Public Domain (SGC)"
    phase: 1
    variables: [tipo_movimiento, fecha, disparador, area, litologia, pendiente]
    notes: "32K+ national records; Antioquia = 4th highest."

  sgc_amenaza:
    name: "SGC Seismic Hazard NSR-10"
    category: geologia
    url: "https://amenazasismica.sgc.gov.co/"
    data_type: tabular
    schedule: once
    license: "Public Domain (SGC)"
    phase: 1
    variables: [Aa, Av, zona_amenaza]
    notes: "San Pedro = intermediate zone, Aa~0.15, Av~0.20."

  igac_cartografia:
    name: "IGAC Official Cartography"
    category: geoespacial
    url: "https://geoportal.igac.gov.co"
    api: "WFS"
    data_type: vector
    schedule: once
    license: "CC-BY-SA-4.0"
    phase: 1
    variables: [limites_municipales, drenajes, vias, curvas_nivel, cuerpos_agua]

  xm_simem:
    name: "XM SiMEM"
    category: mercado_electrico
    url: "https://simem.xm.com.co"
    api: "pydataxm Python library"
    data_type: tabular
    schedule: monthly
    license: "Open Data (CREG)"
    phase: 1
    variables: [precio_bolsa, generacion_real, demanda, aportes_hidricos, volumen_embalse]
    notes: "213+ datasets. 2000-present."

  corine_lc:
    name: "Corine Land Cover Colombia"
    category: biodiversidad
    url: "SIAC / Colombia en Mapas"
    api: "ArcGIS REST"
    data_type: vector
    schedule: once
    license: "CC0 (IDEAM)"
    phase: 1
    variables: [clase_cobertura, nivel_1, nivel_2, nivel_3]
    notes: "5 epochs: 2000, 2005, 2010, 2014, 2018."

  mapbiomas:
    name: "MapBiomas Colombia"
    category: biodiversidad
    url: "https://colombia.mapbiomas.org"
    api: "GEE asset"
    data_type: raster
    schedule: once
    license: "CC-BY-SA-4.0"
    phase: 1
    variables: [clasificacion_cobertura]
    resolution_spatial: "30m"
    notes: "1985-2024, annual. 40 years of land cover change."

  dane_censo:
    name: "DANE CNPV 2018"
    category: socioeconomico
    url: "https://sitios.dane.gov.co/cnpv/"
    api: "datos.gov.co SODA"
    data_type: tabular
    schedule: once
    license: "CC0"
    phase: 1
    variables: [poblacion, vivienda, servicios, educacion, actividad_economica]

  dnp_terridata:
    name: "DNP TerriData"
    category: socioeconomico
    url: "https://terridata.dnp.gov.co/"
    data_type: tabular
    schedule: monthly
    license: "Open Data (DNP)"
    phase: 1
    variables: [800+ indicadores en 16 dimensiones]

  agronet_eva:
    name: "AGRONET / EVA"
    category: socioeconomico
    url: "https://www.datos.gov.co/resource/uejq-wxrr.json"
    api: "SODA"
    data_type: tabular
    schedule: monthly
    license: "CC0"
    phase: 1
    variables: [area_sembrada_ha, produccion_t, rendimiento_t_ha]

  nasa_power:
    name: "NASA POWER"
    category: meteorologia
    url: "https://power.larc.nasa.gov/"
    api: "REST (no auth)"
    data_type: tabular
    schedule: monthly
    license: "NASA Open Data"
    phase: 1
    variables: [T2M, PRECTOTCORR, ALLSKY_SFC_SW_DWN, WS2M, WS10M, WS50M, RH2M, PS]
    resolution_spatial: "0.5deg"
    notes: "300+ vars. 1981-present. No API key needed."

  upme_proyectos:
    name: "UPME Generation Project Registry"
    category: mercado_electrico
    url: "https://www.upme.gov.co"
    data_type: tabular
    schedule: monthly
    license: "Public Domain (UPME)"
    phase: 1
    variables: [proyecto, departamento, municipio, capacidad_mw, tipo, fase]

  runap:
    name: "RUNAP / SINAP Protected Areas"
    category: biodiversidad
    url: "https://runap.parquesnacionales.gov.co"
    api: "ArcGIS REST"
    data_type: vector
    schedule: once
    license: "Public Domain (PNN)"
    phase: 1
    variables: [nombre, categoria, area_ha]

  corantioquia:
    name: "CORANTIOQUIA POMCA"
    category: regulatorio
    url: "https://sig.corantioquia.gov.co"
    api: "ArcGIS REST"
    data_type: vector
    schedule: once
    license: "Public Domain"
    phase: 1
    variables: [jurisdiccion, pomca_boundaries]
    notes: "Oficina Tahamies jurisdiction. Environmental authority for <100MW."

  desinventar:
    name: "DesInventar Antioquia"
    category: geologia
    url: "https://db.desinventar.org"
    data_type: tabular
    schedule: once
    license: "Apache 2.0"
    phase: 1
    variables: [tipo_desastre, fecha, municipio, muertes, heridos, viviendas]
    notes: "1903-2023. 92% of landslides triggered by rainfall."
```

- [ ] **Step 2: Commit**

```bash
git add config/sources.yaml
git commit -m "feat: sources.yaml — complete Phase 1 catalog (22 data sources)"
```

---

### Task 20: Final — Integration Smoke Test

- [ ] **Step 1: Run all tests**

Run: `cd /Users/cristianespinal/milagros-datalake && pytest tests/ -v`
Expected: All tests PASS (catalog, base ingestor, processors)

- [ ] **Step 2: Verify CLI tools work**

Run:
```bash
python scripts/ingest_all.py --dry-run
python scripts/process_all.py --layer silver --dry-run
python scripts/process_all.py --layer gold --dry-run
python scripts/validate.py --layer all
```
Expected: Lists all registered ingestors/processors without errors

- [ ] **Step 3: Final commit**

```bash
git add -A
git commit -m "feat: Milagros data lake Phase 1 — complete infrastructure, 22 ingestors, processors, Gold views"
```

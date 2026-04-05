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

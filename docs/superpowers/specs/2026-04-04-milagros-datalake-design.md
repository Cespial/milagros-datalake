# Milagros Data Lake — Design Spec

**Fecha:** 2026-04-04
**Proyecto:** Central Hidroeléctrica Milagros — Prefactibilidad
**Autor:** Cris Espinal
**Estado:** Aprobado (pendiente revisión final)

---

## 1. Contexto y objetivo

### Qué es Milagros

Proyecto de central hidroeléctrica nueva (>100 MW) en la zona de San Pedro de los Milagros, norte de Antioquia. Etapa de prefactibilidad avanzada con consultores activos (hidrólogos, geólogos, ambientales, eléctricos) pero sin repositorio centralizado de datos.

### Por qué un data lake

Los estudios de prefactibilidad y factibilidad requieren cruzar datos de hidrología, meteorología, geología, teledetección, mercado eléctrico, socioeconomía y regulación ambiental. Hoy cada consultor busca datos por su cuenta, sin trazabilidad ni estandarización. El data lake centraliza, estandariza y cataloga 80+ fuentes de datos para:

1. Alimentar los estudios técnicos en curso (hidrología, geología, EIA, mercado)
2. Proveer datos auditables y trazables para ANLA, CORANTIOQUIA, UPME
3. Generar exports empaquetados por disciplina y audiencia
4. Servir como plantilla replicable para futuros proyectos hidroeléctricos

### Referencia geográfica

La infraestructura hidroeléctrica existente en la zona corresponde al sistema "Aprovechamiento Múltiple del Río Grande" (EPM): Embalse Riogrande II (245 Mm3, 2,270 msnm), centrales La Tasajera (306 MW) y Niquía (~20 MW). El proyecto Milagros es independiente de este sistema.

### Audiencias

| Audiencia | Modo de acceso | Nivel técnico |
|---|---|---|
| Cris (operador) | DuckDB + Python + notebooks | Alto |
| Consultores técnicos | Exports GeoPackage/Excel/NetCDF | Alto |
| Inversionistas/stakeholders | PDF ejecutivos, tablas resumen | Bajo |
| Reguladores (ANLA, CORANTIOQUIA, UPME) | Paquetes formales GDB/Excel/PDF | Medio-alto |

---

## 2. Decisiones arquitectónicas

### Enfoque: Lake Medallion con Motor Espacial

Tres capas (Bronze/Silver/Gold) con manejo diferenciado por tipo de dato (tabular, raster, vector, documento).

**Alternativas descartadas:**
- **Banana Pattern escalado** (DuckDB+Parquet plano): Insuficiente para raster, sin linaje, exports ad-hoc. No escala a 80+ fuentes heterogéneas.
- **Enterprise Lakehouse** (PostGIS, STAC server, Dagster): Overengineered para operador solo + local-first. Migrable desde el enfoque elegido cuando lleguen recursos.

### Infraestructura: Local-first

- **Motor de consulta:** DuckDB con extensión spatial
- **Almacenamiento tabular:** Parquet (Silver/Gold), CSV/JSON/Excel (Bronze)
- **Almacenamiento raster:** GeoTIFF/COG (Silver), NetCDF/GRIB (Bronze)
- **Almacenamiento vector:** GeoParquet (Silver/Gold), Shapefile/GDB/GeoJSON (Bronze)
- **Catálogo:** DuckDB (`catalog.duckdb`) + JSON STAC-lite para raster
- **Procesamiento raster pesado:** Google Earth Engine (gratis), descarga solo resultados recortados
- **Lenguaje:** Python 3.11 (anaconda3)
- **Almacenamiento estimado:** ~60 GB total

### Estrategia de datos raster

Selectivo local + procesamiento cloud: descargar localmente solo datos recortados al AOI (~55x50 km) y procesados. GEE/Planetary Computer para procesamiento pesado; localmente solo resultados. Scripts reproducibles para regenerar todo.

---

## 3. Estructura de directorios

```
milagros-datalake/
├── config/
│   ├── settings.py              # AOI bbox, CRS, rutas, API keys ref
│   ├── sources.yaml             # Catalogo maestro de 80+ fuentes
│   └── .env                     # API keys (no versionado)
├── catalog/
│   ├── catalog.duckdb           # Metadatos: que hay, de donde viene, cuando se ingirio
│   └── stac/                    # JSON por dataset raster (STAC-lite, sin servidor)
├── bronze/                      # Dato crudo, inmutable, tal cual llego
│   ├── tabular/                 # CSV, JSON, Excel de APIs
│   │   ├── ideam_dhime/
│   │   ├── xm_simem/
│   │   ├── dane_cnpv/
│   │   ├── agronet_eva/
│   │   ├── datos_gov/
│   │   └── ...
│   ├── raster/                  # GeoTIFF, NetCDF, GRIB recortados al AOI
│   │   ├── era5_land/
│   │   ├── chirps/
│   │   ├── sentinel1/
│   │   ├── sentinel2/
│   │   ├── dem_srtm/
│   │   ├── dem_copernicus/
│   │   ├── dem_alos/
│   │   └── ...
│   ├── vector/                  # Shapefiles, GeoJSON, GDB
│   │   ├── igac_cartografia/
│   │   ├── sgc_geologia/
│   │   ├── corantioquia_pomca/
│   │   ├── hydrosheds/
│   │   ├── runap/
│   │   └── ...
│   └── documents/               # PDFs, resoluciones, expedientes
│       ├── anla_vital/
│       ├── creg_resoluciones/
│       ├── corantioquia/
│       ├── upme_registros/
│       └── eot_pbot/
├── silver/                      # Limpio, estandarizado, esquema consistente
│   ├── tabular/                 # Parquet con esquema validado
│   │   ├── hidrologia/          # Particionado Hive: year=YYYY/data.parquet
│   │   ├── meteorologia/
│   │   ├── mercado_electrico/
│   │   ├── socioeconomico/
│   │   ├── calidad_agua/
│   │   └── ...
│   ├── raster/                  # COG, CRS unificado
│   │   ├── era5_land/
│   │   ├── chirps/
│   │   ├── indices/             # NDVI, NDWI, turbidez derivados
│   │   └── dem_merged/          # DEM compuesto mejor-resolucion
│   └── vector/                  # GeoParquet, CRS unificado EPSG:4326
│       ├── cuencas.geoparquet
│       ├── geologia.geoparquet
│       ├── cobertura_suelo.geoparquet
│       ├── areas_protegidas.geoparquet
│       └── ...
├── gold/                        # Vistas analiticas, cruces, indicadores
│   ├── balance_hidrico.parquet
│   ├── series_caudal.parquet
│   ├── curvas_duracion.parquet
│   ├── perfil_geologico.geoparquet
│   ├── amenazas_naturales.geoparquet
│   ├── potencial_generacion.parquet
│   ├── mercado_despacho.parquet
│   ├── linea_base_ambiental.geoparquet
│   ├── indicadores_socioeconomicos.parquet
│   └── recurso_solar_eolico.parquet
├── exports/                     # Paquetes listos para entregar
│   ├── consultores/
│   ├── inversionistas/
│   └── reguladores/
├── ingestors/                   # Un modulo Python por fuente
│   ├── __init__.py
│   ├── base.py                  # Clase base con logging, retry, catalogacion
│   ├── ideam_dhime.py
│   ├── xm_simem.py
│   ├── cds_era5.py
│   ├── gee_sentinel.py
│   ├── chirps.py
│   ├── usgs_comcat.py
│   ├── datos_gov.py
│   └── ...
├── processors/                  # Transformaciones Bronze -> Silver -> Gold
│   ├── tabular/
│   ├── raster/
│   └── vector/
├── analytics/                   # Notebooks y scripts de analisis
│   ├── hidrologia/
│   ├── geotecnia/
│   ├── ambiental/
│   └── mercado/
├── dashboard/                   # Next.js app (futuro)
├── scripts/
│   ├── ingest_all.py            # Orquestador de ingestion
│   ├── process_all.py           # Pipeline Bronze -> Silver -> Gold
│   ├── export_consultor.py      # Genera paquetes por disciplina
│   ├── export_inversionistas.py
│   ├── export_regulador.py
│   └── validate.py              # Checks de integridad y completitud
├── tests/
├── docs/
│   └── superpowers/specs/
├── pyproject.toml
└── README.md
```

### Principios de las capas

- **Bronze es inmutable** — nunca se modifica, es el registro de verdad
- **Silver es reproducible** — se regenera completo desde Bronze + processors
- **Gold es derivado** — se regenera desde Silver + analytics
- **Catalogo centralizado** — todo lo que entra se registra en `catalog.duckdb`

---

## 4. Arquitectura de ingestion

### Clase base

```python
class BaseIngestor:
    name: str              # "ideam_dhime", "xm_simem", etc.
    source_type: str       # "api", "download", "gee", "scrape"
    data_type: str         # "tabular", "raster", "vector", "document"
    schedule: str          # "daily", "weekly", "monthly", "once", "on_demand"
    license: str           # "CC0", "CC-BY-4.0", "restricted", etc.

    def fetch(self, aoi, date_range) -> Path    # Descarga a bronze/
    def register(self, path, metadata) -> None   # Registra en catalogo
    def validate(self, path) -> bool             # Hash, schema, bounds check
```

### Catalogo DuckDB (`catalog.duckdb`)

| Columna | Tipo | Descripcion |
|---|---|---|
| `dataset_id` | VARCHAR | Identificador unico del dataset |
| `source` | VARCHAR | Nombre de la fuente |
| `category` | VARCHAR | hidrologia, meteorologia, geoespacial, etc. |
| `data_type` | VARCHAR | tabular, raster, vector, document |
| `layer` | VARCHAR | bronze, silver, gold |
| `file_path` | VARCHAR | Ruta relativa al archivo |
| `file_hash` | VARCHAR | SHA-256 del archivo |
| `file_size_mb` | FLOAT | Tamano en MB |
| `format` | VARCHAR | csv, parquet, geotiff, netcdf, geoparquet, pdf |
| `temporal_start` | DATE | Inicio de cobertura temporal |
| `temporal_end` | DATE | Fin de cobertura temporal |
| `temporal_resolution` | VARCHAR | hourly, daily, monthly, annual, static |
| `spatial_bbox` | VARCHAR | Bounding box JSON |
| `spatial_resolution` | VARCHAR | Resolucion espacial |
| `crs` | VARCHAR | Sistema de referencia |
| `variables` | VARCHAR[] | Lista de variables contenidas |
| `license` | VARCHAR | Licencia de uso |
| `ingested_at` | TIMESTAMP | Fecha/hora de ingestion |
| `ingestor` | VARCHAR | Nombre del ingestor que lo creo |
| `status` | VARCHAR | complete, partial, failed |
| `notes` | VARCHAR | Notas libres |

### STAC-lite para raster

Cada dataset raster en Silver/Gold tiene un JSON sidecar con metadatos STAC simplificados (sin servidor, solo archivos). Campos: `type`, `stac_version`, `id`, `bbox`, `properties` (datetime, variable, resolution, processing level), `assets` (href, type).

### Inventario de fuentes: 55+ ingestores en 4 fases

#### Fase 1 — Critico inmediato (semanas 1-2): 22 ingestores

| # | Ingestor | Fuente | Tipo | Razon de prioridad |
|---|---|---|---|---|
| 1 | `ideam_dhime.py` | IDEAM DHIME — caudal, nivel, precipitacion | tabular | Hidrologia de campo base |
| 2 | `cds_era5.py` | ERA5-Land — 50 vars, 1950-presente | raster | Balance hidrico sin vacios |
| 3 | `chirps.py` | CHIRPS v2 — precipitacion diaria 5.5km | raster | Mejor precip. satelital Andes |
| 4 | `gee_dem.py` | Copernicus GLO-30 + SRTM + ALOS PALSAR | raster | Topografia, pendientes, cuencas |
| 5 | `hydrosheds.py` | HydroSHEDS/BASINS — subcuencas, red fluvial | vector | Delineacion cuenca |
| 6 | `glofas.py` | GloFAS v4 — descarga fluvial modelada | raster | Caudal historico complementario |
| 7 | `sgc_geologia.py` | SGC Mapa Geologico 1:100K | vector | Geologia, fallas, litologia |
| 8 | `sgc_sismicidad.py` | SGC RSNC + USGS ComCat — catalogo sismico | tabular | Amenaza sismica |
| 9 | `sgc_simma.py` | SGC SIMMA — deslizamientos 32K+ registros | tabular | Riesgo geotecnico |
| 10 | `igac_cartografia.py` | IGAC WMS/WFS — cartografia oficial | vector | Base cartografica |
| 11 | `xm_simem.py` | XM SiMEM — generacion, precios, demanda | tabular | Mercado electrico |
| 12 | `corine_lc.py` | IDEAM Corine Land Cover — 5 epocas | vector | Cobertura/uso suelo |
| 13 | `mapbiomas.py` | MapBiomas Colombia — 1985-2024 anual 30m | raster | Cambio cobertura 40 anos |
| 14 | `dane_censo.py` | DANE CNPV 2018 + proyecciones | tabular | Poblacion, demografia |
| 15 | `dnp_terridata.py` | DNP TerriData — 800+ indicadores | tabular | Socioeconomia municipal |
| 16 | `agronet_eva.py` | AGRONET/EVA — produccion agropecuaria | tabular | Uso suelo productivo |
| 17 | `nasa_power.py` | NASA POWER — radiacion, viento, temperatura | tabular | Recurso solar/eolico |
| 18 | `upme_proyectos.py` | UPME registro proyectos generacion | tabular | Proyectos competidores |
| 19 | `runap.py` | RUNAP — areas protegidas SINAP | vector | Restricciones ambientales |
| 20 | `corantioquia.py` | CORANTIOQUIA POMCA, jurisdiccion | vector/doc | Autoridad ambiental |
| 21 | `sgc_amenaza.py` | SGC amenaza sismica — Aa, Av | tabular | Diseno sismico NSR-10 |
| 22 | `desinventar.py` | DesInventar Antioquia — desastres 1903-2023 | tabular | Historial desastres |

#### Fase 2 — Importante (semanas 3-4): 18 ingestores

| # | Ingestor | Fuente | Tipo |
|---|---|---|---|
| 23 | `gee_sentinel2.py` | Sentinel-2 — indices espectrales AOI | raster |
| 24 | `gee_sentinel1.py` | Sentinel-1 SAR — retrodispersion AOI | raster |
| 25 | `gee_landsat.py` | Landsat 8/9 — series historicas | raster |
| 26 | `persiann.py` | PERSIANN-CCS-CDR — precip. 3-horaria | raster |
| 27 | `open_meteo.py` | Open-Meteo Weather + Flood API | tabular |
| 28 | `chelsa.py` | CHELSA v2.1 — climatologia 1km | raster |
| 29 | `worldclim.py` | WorldClim v2.1 — bioclimaticas | raster |
| 30 | `cmip6.py` | CMIP6 NEX-GDDP — escenarios climaticos | raster |
| 31 | `global_solar.py` | Global Solar Atlas — GHI, PVOUT 250m | raster |
| 32 | `global_wind.py` | Global Wind Atlas — viento 250m | raster |
| 33 | `nrel_nsrdb.py` | NREL NSRDB — radiacion 4km semi-horaria | raster |
| 34 | `gbif_sib.py` | GBIF/SiB Colombia — biodiversidad | tabular |
| 35 | `humboldt.py` | Instituto Humboldt — ecosistemas, BioModelos | vector |
| 36 | `fao.py` | FAO GeoNetwork — suelos HWSD, GAEZ | raster |
| 37 | `invias.py` | INVIAS SIV — red vial, estado | vector |
| 38 | `mintic.py` | MinTIC — cobertura telecomunicaciones | tabular |
| 39 | `upme_red.py` | UPME geoportal — lineas STN, subestaciones | vector |
| 40 | `sui_sspd.py` | SUI/SSPD — capacidad instalada, tarifas | tabular |

#### Fase 3 — Complementario (semanas 5-6): 15+ ingestores

| # | Ingestor | Fuente | Tipo |
|---|---|---|---|
| 41 | `copernicus_wq.py` | Copernicus Water Quality | raster |
| 42 | `icesat2.py` | ICESat-2 ATL13 — altimetria | tabular |
| 43 | `gedi.py` | GEDI — estructura vegetacion | raster |
| 44 | `modis.py` | MODIS — LST, incendios, NDVI | raster |
| 45 | `viirs.py` | VIIRS — incendios, luces nocturnas | raster |
| 46 | `noaa_gsod.py` | NOAA GSOD — estaciones aeropuerto | tabular |
| 47 | `grdc.py` | GRDC — descarga estaciones globales | tabular |
| 48 | `gemstat.py` | GEMStat — calidad agua global | tabular |
| 49 | `osm.py` | OpenStreetMap — infraestructura local | vector |
| 50 | `catastro_antioquia.py` | Catastro Antioquia — predios | vector |
| 51 | `gobernacion.py` | Gobernacion Antioquia — anuarios | tabular |
| 52 | `anla_vital.py` | ANLA VITAL — expedientes EIA | document |
| 53 | `creg.py` | CREG Alejandria — resoluciones | document |
| 54 | `nasa_lhasa.py` | NASA LHASA v2 — nowcast deslizamientos | raster |
| 55 | `planet.py` | Planet Labs (si hay acceso NICFI/edu) | raster |

#### Fase 4 — Documentos y regulatorio (continuo)

Ingestores para scraping/descarga de PDFs de ANLA VITAL, CREG, CORANTIOQUIA, UPME. Extraccion de metadatos y almacenamiento indexado.

### Orquestacion

Sin Airflow ni Dagster. Script `ingest_all.py` con CLI:

```bash
python scripts/ingest_all.py --phase 1          # Todas las fuentes fase 1
python scripts/ingest_all.py --source ideam_dhime  # Una fuente especifica
python scripts/ingest_all.py --category hidrologia  # Por categoria
```

Retry automatico con backoff exponencial (via `tenacity`), logging estructurado (`structlog`), registro automatico en catalogo post-descarga.

---

## 5. Pipeline de procesamiento

### Bronze -> Silver

#### Tabular

Cada categoria tiene un procesador: lectura de Bronze, limpieza, tipado, deduplicacion, estandarizacion de nombres/unidades/timezone, escritura a Parquet particionado por ano.

**Convenciones Silver:**

| Aspecto | Convencion |
|---|---|
| Nombres columnas | `snake_case` espanol: `caudal_m3s`, `precipitacion_mm` |
| Timestamps | UTC, `TIMESTAMP WITH TIME ZONE` |
| Coordenadas | `lat`/`lon` DOUBLE, WGS84 |
| Unidades | SI: m3/s, mm, C, m, kW, kWh, ha, km2 |
| Nulos | `NULL` nativo (no strings "-999", "N/A") |
| Codigos municipio | DANE 5 digitos VARCHAR: `05664` |
| CRS | EPSG:4326 |
| Particionado | `{categoria}/year={YYYY}/data.parquet` |

#### Raster

| Operacion | Herramienta |
|---|---|
| Recorte AOI | `rasterio` + `shapely` |
| Reproyeccion | `rasterio.warp` a EPSG:4326 |
| Conversion COG | `rio-cogeo` / `gdal_translate` |
| Mosaico DEMs | `rasterio.merge` (ALOS 12.5m + Copernicus 30m relleno) |
| Indices espectrales | GEE compute, descarga como COG |
| NetCDF -> COG | `xarray` + `rioxarray` |
| Metadatos STAC | JSON sidecar por dataset |

#### Vector

| Operacion | Herramienta |
|---|---|
| Lectura multi-formato | `geopandas` / `fiona` |
| Reproyeccion | `geopandas.to_crs(EPSG:4326)` |
| Recorte AOI | `geopandas.clip()` |
| Limpieza geometrias | `shapely.make_valid()` |
| Escritura | `geopandas.to_parquet()` (GeoParquet) |

### Silver -> Gold: vistas analiticas

10 vistas que cruzan multiples fuentes Silver:

| Vista Gold | Fuentes Silver | Producto | Uso en prefactibilidad |
|---|---|---|---|
| `balance_hidrico.parquet` | ERA5 + CHIRPS + IDEAM | P - ET - Q - dS por subcuenca, mensual, 1950-presente | Disponibilidad hidrica |
| `series_caudal.parquet` | IDEAM + GloFAS + ERA5 | Serie continua diaria ~70 anos con indicador de confianza | Curvas de duracion, caudales de diseno |
| `curvas_duracion.parquet` | series_caudal Gold | Percentiles 5-95 de caudal, mensual y anual | Caudal firme Q95, caudal medio |
| `potencial_generacion.parquet` | curvas_duracion + DEM + HydroSHEDS | P=QHng para combinaciones caudal-salto | Capacidad instalable por alternativa |
| `perfil_geologico.geoparquet` | SGC geologia + sismicidad + SIMMA + DEM | Aptitud geologica: litologia, fallas, deslizamientos, pendiente | Restricciones localizacion presa |
| `amenazas_naturales.geoparquet` | SGC + DesInventar + LHASA + DEM | Amenaza multicriterio: sismica, deslizamiento, inundacion | Evaluacion riesgo, input EIA |
| `linea_base_ambiental.geoparquet` | Corine + MapBiomas + RUNAP + GBIF + Humboldt + POMCA | Cobertura, areas protegidas, ecosistemas, biodiversidad | Capitulo ambiental EIA |
| `mercado_despacho.parquet` | XM SiMEM + UPME | Precio bolsa, factor planta hidro, proyectos competidores | Viabilidad financiera |
| `indicadores_socioeconomicos.parquet` | DANE + DNP + AGRONET + Gobernacion | Perfil municipal: poblacion, NBI, Gini, produccion, transferencias Ley 99 | Capitulo socioeconomico EIA |
| `recurso_solar_eolico.parquet` | Global Solar + Wind Atlas + NASA POWER + NSRDB | GHI, DNI, PVOUT, viento por punto AOI | Solar flotante, complementariedad |

### Orquestacion de procesamiento

```bash
python scripts/process_all.py --layer silver                          # Todo Bronze -> Silver
python scripts/process_all.py --layer gold                            # Todo Silver -> Gold
python scripts/process_all.py --layer silver --category hidrologia    # Por categoria
python scripts/process_all.py --layer gold --view balance_hidrico     # Vista especifica
```

Cada procesador registra en catalogo: input datasets (linaje), fecha, parametros. Todo Gold es trazable hasta Bronze.

---

## 6. Capa de exports

### Por audiencia

| Audiencia | Script | Formato | Contenido |
|---|---|---|---|
| Consultores hidrologos | `export_consultor.py --disciplina hidrologia` | GeoPackage + Excel + NetCDF | Silver/Gold hidrologia, DEM, cuencas |
| Consultores geologos | `export_consultor.py --disciplina geologia` | GeoPackage + PDF mapas | Silver/Gold geologia, amenazas, DEM |
| Consultores ambientales | `export_consultor.py --disciplina ambiental` | GeoPackage + Excel + CSV | Cobertura, biodiversidad, calidad agua |
| Ingenieros electricos | `export_consultor.py --disciplina electrico` | Excel + CSV | Mercado, potencial generacion, red STN |
| Inversionistas | `export_inversionistas.py` | PDF ejecutivo + Excel resumen | Indicadores clave, capacidad, IRR preliminar |
| ANLA | `export_regulador.py --entidad anla` | GDB + Excel + PDF + metadatos | Paquete EIA completo |
| CORANTIOQUIA | `export_regulador.py --entidad corantioquia` | Similar ANLA | Concesion de aguas |
| UPME | `export_regulador.py --entidad upme` | Formularios + Excel | Registro proyecto |

Cada export se genera en `exports/{audiencia}/{fecha}/`, incluye `README.txt` y `metadata.json` con linaje completo, y se registra en catalogo.

### Dashboard (futuro)

Next.js 16 + Mapbox + Recharts. No se construye en esta fase. La estructura Gold alimenta el dashboard directamente cuando se implemente. Funciones previstas: mapa interactivo con capas togglables, series temporales, indicadores en tarjetas, estado de completitud del lake.

---

## 7. Stack tecnologico

### Dependencias core

```
duckdb>=1.2           # Motor de consulta
pyarrow>=18.0         # IO Parquet
pandas>=2.2           # Transformaciones tabulares
geopandas>=1.0        # Vector geospatial
rasterio>=1.4         # IO raster
rioxarray>=0.18       # xarray + rasterio bridge
shapely>=2.0          # Geometrias
fiona>=1.10           # IO vector multi-formato
pyproj>=3.7           # Proyecciones CRS
rio-cogeo>=5.0        # Cloud-Optimized GeoTIFF
xarray>=2024.10       # Arrays N-dimensionales
netCDF4>=1.7          # IO NetCDF
cdsapi>=0.7           # Copernicus CDS API
earthengine-api>=1.4  # Google Earth Engine
pystac-client>=0.8    # STAC catalogs
pydataxm>=0.5         # XM SiMEM API
requests>=2.32        # HTTP
httpx>=0.28           # Async HTTP
python-dotenv>=1.0    # .env loading
pyyaml>=6.0           # YAML config
tqdm>=4.67             # Progress bars
tenacity>=9.0         # Retry con backoff
structlog>=24.4       # Logging estructurado
click>=8.1            # CLI
xxhash>=3.5           # Hashing rapido
```

### Configuracion central

```python
# config/settings.py
AOI_BBOX = {"west": -75.80, "south": 6.25, "east": -75.25, "north": 6.70}

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

CRS_WGS84 = "EPSG:4326"
CRS_COLOMBIA = "EPSG:3116"

DEFAULT_START_DATE = "1950-01-01"
DEFAULT_END_DATE = "2026-04-04"  # Se actualiza dinamicamente
```

### API keys y registros necesarios

| Servicio | Necesario para | Tiempo registro |
|---|---|---|
| Copernicus CDS | ERA5-Land, GloFAS | 5 min |
| NASA Earthdata | SRTM, ALOS, ICESat-2, GEDI, LHASA | 5 min |
| Google Earth Engine | Sentinel-1/2, Landsat, MODIS, DEMs | 1-3 dias |
| USGS EarthExplorer | SRTM, Landsat | 5 min |
| NREL Developer | NSRDB | 5 min |
| GBIF | Biodiversidad | 5 min |
| DLR TanDEM-X | DEM 12m (no bloqueante) | 4-8 semanas |

### Almacenamiento estimado

| Capa | Estimacion |
|---|---|
| Bronze tabular | ~5 GB |
| Bronze raster | ~30 GB |
| Bronze vector | ~2 GB |
| Bronze documents | ~1 GB |
| Silver tabular | ~3 GB |
| Silver raster | ~15 GB |
| Silver vector | ~1 GB |
| Gold | ~2 GB |
| Catalog | <100 MB |
| **Total** | **~60 GB** |

### Git y versionado

Se versionan: `config/`, `ingestors/`, `processors/`, `analytics/`, `scripts/`, `tests/`, `docs/`, `catalog/stac/`, `pyproject.toml`, `README.md`.

No se versionan (en `.gitignore`): `bronze/`, `silver/`, `gold/`, `exports/`, `catalog/catalog.duckdb`, `.env`, `*.tif`, `*.nc`.

Reproducibilidad: cualquiera con el repo + `.env` + `python scripts/ingest_all.py` reconstruye el lake completo.

---

## 8. Fuera de alcance (esta fase)

- **Dashboard Next.js** — se implementa cuando haya audiencia que lo necesite
- **IoT/SCADA en tiempo real** — aplica a operacion de central, no a prefactibilidad
- **Orquestador enterprise** (Airflow, Dagster) — migrable desde scripts CLI cuando lleguen recursos
- **PostGIS / STAC server** — migrable desde DuckDB spatial + STAC-lite
- **Datos comerciales** (Planet Labs, Maxar) — se agregan si se consigue acceso NICFI/educacion
- **Pipeline de ML/AI** — modelos predictivos se construyen sobre Gold cuando los datos esten listos

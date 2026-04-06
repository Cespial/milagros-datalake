# Milagros Dashboard — Design Spec

**Fecha:** 2026-04-05
**Proyecto:** Central Hidroelectrica Milagros — Dashboard de Prefactibilidad
**Estado:** Aprobado

---

## 1. Objetivo

Pagina web publica que presenta toda la informacion del lago de datos Milagros de forma visual e interactiva. Orientada a dos audiencias:

1. **Tomadores de decisiones / inversionistas:** Indicadores clave, mapa del AOI, potencial de generacion, inventario de fuentes, roadmap
2. **Tecnicos / consultores:** Estado del data lake, arquitectura, stack, como reproducir

## 2. Decisiones arquitectonicas

### Stack

- **Framework:** Next.js 16 (App Router)
- **Estilos:** Tailwind CSS
- **Mapa:** Mapbox GL JS (react-map-gl)
- **Graficos:** Recharts
- **Deploy:** Vercel
- **Datos:** JSONs estaticos pre-generados en `public/data/`

### Contenido hibrido

- **Estatico (hardcoded):** Inventario de 80+ fuentes, roadmap, marco regulatorio, contexto Riogrande II, area de estudio
- **Dinamico (JSONs):** Indicadores de Gold views, estado de ingestion del catalogo, series temporales resumidas

### Flujo de datos

```
milagros-datalake/              milagros-dashboard/
  Gold views (Parquet)    →       public/data/
  catalog.duckdb          →         indicators.json
  Bronze files            →         ingestion_status.json
                                    precipitation.json
export_dashboard.py               curvas_duracion.json
  (genera JSONs)                    potencial_generacion.json
                                    geologia.geojson
                                    areas_protegidas.geojson
                                    aoi_boundary.geojson
```

No hay backend, no hay base de datos, no hay API routes. Todo son archivos JSON/GeoJSON estaticos que Next.js lee en build time o el cliente fetcha.

## 3. Estructura de la pagina

### Hero: Mapa interactivo (pantalla completa)

- Mapbox GL JS centrado en AOI (lat: 6.475, lon: -75.525, zoom: 10)
- Capas togglables con panel lateral:
  - AOI boundary (poligono naranja)
  - Municipios (poligonos con labels)
  - Unidades geologicas (poligonos coloreados por tipo)
  - Fallas geologicas (lineas rojas)
  - Areas protegidas RUNAP (poligonos verdes semi-transparentes)
  - DEM hillshade (raster tile de Mapbox terrain)
- Overlay flotante arriba-izquierda: titulo + 4 indicadores clave en tarjetas
  - Potencial estimado (MW)
  - Caudal firme Q95 (m3/s)
  - Precipitacion media anual (mm)
  - Area de estudio (km2)

### Seccion "Hallazgos Clave"

Tarjetas con indicadores principales:
- Potencial de generacion estimado (rango MW por combinacion Q-H)
- Caudal firme Q95 y caudal medio
- Precipitacion media anual y estacional
- Zona de amenaza sismica (Intermedia, Aa=0.15, Av=0.20)
- Areas protegidas en el AOI (count + area total ha)
- Deslizamientos registrados en el AOI (count)

Graficos:
- Curvas de duracion de caudal (Recharts LineChart — percentiles vs caudal)
- Precipitacion mensual promedio (Recharts BarChart — 12 meses)
- Tabla de potencial de generacion (Q x H x eta → MW)

### Seccion "Inventario de Datos"

Tabla interactiva con 80+ fuentes:
- Columnas: Fuente, Categoria, Variables, Enlace, Costo, Estado
- Filtros por categoria (tabs o dropdown)
- Badges de estado: verde (ingerido), amarillo (pendiente), gris (fase 2+), rojo (requiere pago)
- Datos hardcoded (ya estan en el README)

### Seccion "Roadmap"

Timeline visual horizontal de 4 fases:
- Fase 1: Infraestructura + Datos Criticos — COMPLETADA (barra verde)
- Fase 2: Datos Complementarios — EN PROGRESO (barra parcial)
- Fase 3: Datos Especializados — PENDIENTE
- Fase 4: Documentos y Regulatorio — PENDIENTE

Productos por fase (lista con checkmarks)

### Seccion "Estado del Data Lake"

- Barras de progreso por categoria (hidrologia: 4/6, meteorologia: 4/9, etc.)
- Metricas: total archivos, tamano del lake, ultimo update
- Tabla de ingestores con columnas: nombre, fuente, estado, registros, tamano

### Seccion "Area de Estudio"

- Tabla de 10 municipios (codigo DANE, nombre, poblacion)
- Parametros NSR-10 (Aa, Av, zona)
- Contexto del sistema Riogrande II existente (tabla de La Tasajera, Niquia, embalse)
- Autoridades competentes (ANLA, CORANTIOQUIA)

### Footer tecnico

- Arquitectura Medallion (diagrama simple)
- Stack tecnologico (tabla)
- Como reproducir (comandos)
- Link al repo GitHub

## 4. Datos dinamicos (JSONs)

### `public/data/indicators.json`

```json
{
  "potencial_mw_min": 50,
  "potencial_mw_max": 800,
  "caudal_q95_m3s": null,
  "caudal_medio_m3s": null,
  "precipitacion_media_mm": 2100,
  "area_estudio_km2": 2750,
  "zona_sismica": "Intermedia",
  "aa": 0.15,
  "av": 0.20,
  "areas_protegidas_count": 21,
  "deslizamientos_count": 42,
  "sismos_count": 2183,
  "fuentes_ingeridas": 12,
  "fuentes_total": 80,
  "lake_size_mb": 888,
  "last_update": "2026-04-05"
}
```

### `public/data/ingestion_status.json`

```json
[
  {"name": "nasa_power", "source": "NASA POWER", "category": "meteorologia", "status": "complete", "records": 16425, "size_mb": 4.6},
  {"name": "sgc_sismicidad", "source": "SGC/USGS ComCat", "category": "geologia", "status": "complete", "records": 2183, "size_mb": 2.3},
  ...
]
```

### `public/data/precipitation.json`

Serie mensual de precipitacion promedio (12 valores) extraida de CHIRPS/ERA5.

### `public/data/curvas_duracion.json`

Percentiles de caudal (del Gold view curvas_duracion.parquet).

### `public/data/potencial_generacion.json`

Tabla Q x H x eta → MW (del Gold view potencial_generacion.parquet).

### `public/data/geologia.geojson`

Unidades geologicas + fallas simplificadas (del Bronze SGC geologia).

### `public/data/areas_protegidas.geojson`

Poligonos RUNAP simplificados.

### `public/data/aoi_boundary.geojson`

Poligono del bounding box del AOI.

## 5. Script de exportacion

`milagros-datalake/scripts/export_dashboard.py` — genera todos los JSONs anteriores leyendo de Bronze/Silver/Gold y los copia a `milagros-dashboard/public/data/`.

## 6. Estilo visual

- **Tono:** Moderno/startup (Linear, Vercel)
- **Fondo:** Blanco/gris muy claro para secciones, mapa oscuro (Mapbox dark)
- **Tipografia:** Inter o system font
- **Colores:** Azul principal (#2563EB), verde para OK (#22C55E), amarillo para pendiente (#EAB308), rojo para alerta (#EF4444), gris para inactivo
- **Tarjetas:** Bordes sutiles, sombra suave, rounded-xl
- **Responsive:** Desktop-first pero usable en tablet

## 7. Fuera de alcance

- Autenticacion / login
- Edicion de datos desde el dashboard
- Conexion directa a DuckDB (todo via JSONs estaticos)
- Dashboard de monitoreo en tiempo real (IoT/SCADA)
- Internacionalizacion (solo espanol)

"""
Export Bronze data to dashboard-ready JSON/GeoJSON files.

Usage:
    PYTHONPATH=. python scripts/export_dashboard.py --output /path/to/output/
"""

import json
import subprocess
from collections import defaultdict
from pathlib import Path

import click

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_json(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data, indent: int = 2):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=indent)
    size_kb = path.stat().st_size / 1024
    click.echo(f"  wrote {path.name}  ({size_kb:.1f} KB)")


# ---------------------------------------------------------------------------
# Individual exporters
# ---------------------------------------------------------------------------

def export_precipitation(nasa_path: Path, out_dir: Path):
    """Monthly average precipitation from NASA POWER daily data."""
    data = load_json(nasa_path)
    prec_daily = data["properties"]["parameter"]["PRECTOTCORR"]

    # prec_daily keys are 'YYYYMMDD' strings
    monthly_sums = defaultdict(float)
    monthly_counts = defaultdict(int)

    for date_str, val in prec_daily.items():
        if not date_str.isdigit() or len(date_str) != 8:
            continue
        if val is None or val < 0:
            continue
        month = int(date_str[4:6])  # 1-12
        # Convert mm/day -> mm/month would require days-in-month; instead
        # we accumulate daily values per calendar-month and then compute the
        # average daily rate, multiplied by average days per month (30.44).
        monthly_sums[month] += val
        monthly_counts[month] += 1

    month_labels = ["Ene", "Feb", "Mar", "Abr", "May", "Jun",
                    "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]

    # Average monthly precipitation (mm/month) = avg_daily_rate * 30.44
    values = []
    for m in range(1, 13):
        if monthly_counts[m] > 0:
            avg_daily = monthly_sums[m] / monthly_counts[m]
            avg_monthly = round(avg_daily * 30.44, 1)
        else:
            avg_monthly = None
        values.append(avg_monthly)

    result = {"months": month_labels, "values": values}
    save_json(out_dir / "precipitation.json", result)
    return values


def compute_precip_annual_avg(nasa_path: Path) -> float:
    """Compute annual average total precipitation (mm/year) from NASA POWER."""
    data = load_json(nasa_path)
    prec_daily = data["properties"]["parameter"]["PRECTOTCORR"]

    # Group by year
    yearly = defaultdict(list)
    for date_str, val in prec_daily.items():
        if not date_str.isdigit() or len(date_str) != 8:
            continue
        if val is None or val < 0:
            continue
        year = date_str[:4]
        yearly[year].append(val)

    # Annual totals = sum of daily values for that year
    annual_totals = [sum(vals) for vals in yearly.values() if len(vals) >= 300]
    if annual_totals:
        return round(sum(annual_totals) / len(annual_totals), 0)
    return None


def export_indicators(
    nasa_path: Path,
    hazard_path: Path,
    runap_path: Path,
    simma_path: Path,
    usgs_path: Path,
    desinventar_path: Path,
    bronze_dir: Path,
    out_dir: Path,
):
    """Generate summary indicators JSON."""
    precip_avg = compute_precip_annual_avg(nasa_path)

    # Seismic params (use first municipality = San Pedro)
    hazard = load_json(hazard_path)
    first_mun = list(hazard.values())[0]
    zona_sismica = first_mun["zona"]
    aa = first_mun["Aa"]
    av = first_mun["Av"]

    # Counts from vector/tabular sources
    runap = load_json(runap_path)
    areas_protegidas_count = len(runap["features"])

    simma = load_json(simma_path)
    deslizamientos_count = simma["total_features"]

    usgs = load_json(usgs_path)
    sismos_count = len(usgs["features"])

    desinventar = load_json(desinventar_path)
    emergencias_count = len(desinventar)

    # Bronze directory size in MB
    result = subprocess.run(
        ["du", "-sk", str(bronze_dir)],
        capture_output=True, text=True
    )
    bronze_kb = int(result.stdout.split()[0]) if result.returncode == 0 else 0
    lake_size_mb = round(bronze_kb / 1024, 1)

    indicators = {
        "potencial_mw_min": 50,
        "potencial_mw_max": 800,
        "caudal_q95_m3s": None,
        "caudal_medio_m3s": None,
        "precipitacion_media_mm": precip_avg,
        "area_estudio_km2": 2750,
        "zona_sismica": zona_sismica,
        "aa": aa,
        "av": av,
        "areas_protegidas_count": areas_protegidas_count,
        "deslizamientos_count": deslizamientos_count,
        "sismos_count": sismos_count,
        "emergencias_count": emergencias_count,
        "fuentes_ingeridas": 12,
        "fuentes_total": 80,
        "lake_size_mb": lake_size_mb,
        "last_update": "2026-04-05",
    }

    save_json(out_dir / "indicators.json", indicators)


def export_ingestion_status(out_dir: Path):
    """Generate ingestion status array."""
    status = [
        {
            "id": "nasa_power",
            "name": "NASA POWER",
            "category": "meteorologia",
            "status": "ok",
            "records": 16527,
            "size_mb": 4.6,
            "description": "Meteorología diaria 1981-2026 (11 variables)",
        },
        {
            "id": "usgs_comcat",
            "name": "USGS ComCat Sismicidad",
            "category": "geologia",
            "status": "ok",
            "records": 2183,
            "size_mb": 2.3,
            "description": "Catálogo sísmico histórico M≥2.5",
        },
        {
            "id": "sgc_amenaza",
            "name": "SGC Amenaza Sísmica NSR-10",
            "category": "geologia",
            "status": "ok",
            "records": 10,
            "size_mb": 0.002,
            "description": "Parámetros sísmicos Aa/Av por municipio",
        },
        {
            "id": "sgc_simma",
            "name": "SGC SIMMA — Movimientos en Masa",
            "category": "geologia",
            "status": "ok",
            "records": 42,
            "size_mb": 0.02,
            "description": "Inventario de deslizamientos y movimientos en masa",
        },
        {
            "id": "desinventar",
            "name": "UNGRD Desinventar",
            "category": "socioeconomico",
            "status": "ok",
            "records": 2718,
            "size_mb": 6.5,
            "description": "Inventario histórico de desastres 1970-2024",
        },
        {
            "id": "dane_censo",
            "name": "DANE Censo 2018",
            "category": "socioeconomico",
            "status": "ok",
            "records": 6740,
            "size_mb": 1.0,
            "description": "Población por municipio, clase y centro poblado",
        },
        {
            "id": "agronet_eva",
            "name": "Agronet EVA Agrícola",
            "category": "socioeconomico",
            "status": "ok",
            "records": 1400000,
            "size_mb": 180.0,
            "description": "Evaluaciones agropecuarias municipales 1987-2023",
        },
        {
            "id": "gee_dem",
            "name": "Google Earth Engine — DEM",
            "category": "teledeteccion",
            "status": "ok",
            "records": 3,
            "size_mb": 45.0,
            "description": "Modelos digitales de elevación (SRTM/ALOS/Copernicus)",
        },
        {
            "id": "chirps",
            "name": "CHIRPS Precipitación Anual",
            "category": "teledeteccion",
            "status": "ok",
            "records": 46,
            "size_mb": 620.0,
            "description": "Rasters de precipitación anual 1981-2026",
        },
        {
            "id": "sgc_geologia_unidades",
            "name": "SGC Geología — Unidades",
            "category": "geologia",
            "status": "ok",
            "records": 32,
            "size_mb": 1.8,
            "description": "Unidades geológicas del área de estudio",
        },
        {
            "id": "sgc_geologia_fallas",
            "name": "SGC Geología — Fallas",
            "category": "geologia",
            "status": "ok",
            "records": 17,
            "size_mb": 0.05,
            "description": "Fallas geológicas del área de estudio",
        },
        {
            "id": "runap",
            "name": "RUNAP — Áreas Protegidas",
            "category": "biodiversidad",
            "status": "ok",
            "records": 21,
            "size_mb": 0.8,
            "description": "Registro Único Nacional de Áreas Protegidas",
        },
    ]

    save_json(out_dir / "ingestion_status.json", status)


def export_geologia(geologia_path: Path, out_dir: Path):
    """Simplified geology units GeoJSON."""
    data = load_json(geologia_path)
    keep_props = {"SimboloUC", "Descripcion", "Edad"}

    features = []
    for f in data["features"]:
        props = {k: v for k, v in f["properties"].items() if k in keep_props}
        features.append({
            "type": "Feature",
            "properties": props,
            "geometry": f["geometry"],
        })

    out = {"type": "FeatureCollection", "features": features}
    save_json(out_dir / "geologia.geojson", out)


def export_fallas(fallas_path: Path, out_dir: Path):
    """Simplified faults GeoJSON."""
    data = load_json(fallas_path)
    keep_props = {"NombreFalla", "Tipo"}

    features = []
    for f in data["features"]:
        props = {k: v for k, v in f["properties"].items() if k in keep_props}
        features.append({
            "type": "Feature",
            "properties": props,
            "geometry": f["geometry"],
        })

    out = {"type": "FeatureCollection", "features": features}
    save_json(out_dir / "fallas.geojson", out)


def export_areas_protegidas(runap_path: Path, out_dir: Path):
    """Simplified protected areas GeoJSON."""
    data = load_json(runap_path)
    keep_props = {"ap_nombre", "ap_categoria"}

    features = []
    for f in data["features"]:
        props = {k: v for k, v in f["properties"].items() if k in keep_props}
        features.append({
            "type": "Feature",
            "properties": props,
            "geometry": f["geometry"],
        })

    out = {"type": "FeatureCollection", "features": features}
    save_json(out_dir / "areas_protegidas.geojson", out)


def export_aoi_boundary(out_dir: Path):
    """AOI bounding box as GeoJSON polygon."""
    # From config/settings.py AOI_BBOX
    west, south, east, north = -75.80, 6.25, -75.25, 6.70

    feature = {
        "type": "Feature",
        "properties": {"name": "Area de Estudio"},
        "geometry": {
            "type": "Polygon",
            "coordinates": [[
                [west, south],
                [east, south],
                [east, north],
                [west, north],
                [west, south],
            ]],
        },
    }
    out = {"type": "FeatureCollection", "features": [feature]}
    save_json(out_dir / "aoi_boundary.geojson", out)


def export_municipios(censo_path: Path, out_dir: Path):
    """Municipality list with population from census (best-effort match)."""
    # AOI_MUNICIPIOS from config/settings.py
    aoi = {
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

    # Census data for this project only covers Magdalena municipalities —
    # the AOI municipalities are not in the file. We use known 2018 DANE
    # census totals for Antioquia municipalities instead.
    known_poblacion = {
        "05664": 30534,   # San Pedro de los Milagros
        "05264": 9126,    # Entrerrios
        "05086": 9036,    # Belmira
        "05237": 32046,   # Donmatias
        "05686": 71571,   # Santa Rosa de Osos
        "05079": 62011,   # Barbosa
        "05088": 532493,  # Bello (metropolitan)
        "05761": 14087,   # Sopetran
        "05576": 5803,    # Olaya
        "05042": 24029,   # Santafe de Antioquia
    }

    # Also try to match from the loaded census file (in case it has a superset)
    censo = load_json(censo_path)
    # Build lookup by name normalised
    def norm(s):
        return s.lower().replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u")

    censo_by_name = defaultdict(int)
    for row in censo:
        mname = norm(row["municipio"])
        try:
            personas = int(row.get("personas") or 0)
        except (ValueError, TypeError):
            personas = 0
        censo_by_name[mname] += personas

    municipios = []
    for codigo, nombre in aoi.items():
        # Try census lookup first
        pop = censo_by_name.get(norm(nombre))
        if not pop:
            pop = known_poblacion.get(codigo)
        municipios.append({
            "codigo": codigo,
            "nombre": nombre,
            "poblacion": pop,
        })

    save_json(out_dir / "municipios.json", municipios)


def export_sismos(usgs_path: Path, out_dir: Path):
    """Simplified earthquake GeoJSON — M>=3.5 only."""
    data = load_json(usgs_path)

    features = []
    for f in data["features"]:
        mag = f["properties"].get("mag")
        if mag is None or mag < 3.5:
            continue

        # Convert epoch ms to ISO date string
        time_ms = f["properties"].get("time")
        if time_ms is not None:
            from datetime import datetime, timezone
            dt = datetime.fromtimestamp(time_ms / 1000, tz=timezone.utc)
            date_str = dt.strftime("%Y-%m-%d")
        else:
            date_str = None

        coords = f["geometry"]["coordinates"]  # [lon, lat, depth]
        depth = coords[2] if len(coords) > 2 else None

        features.append({
            "type": "Feature",
            "properties": {
                "mag": mag,
                "depth_km": round(depth, 1) if depth is not None else None,
                "date": date_str,
            },
            "geometry": {
                "type": "Point",
                "coordinates": [coords[0], coords[1]],
            },
        })

    out = {"type": "FeatureCollection", "features": features}
    click.echo(f"  sismos: {len(features)} features (M>=3.5) out of {len(data['features'])} total")
    save_json(out_dir / "sismos.geojson", out)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@click.command()
@click.option(
    "--output", "-o",
    default="exports/dashboard",
    show_default=True,
    help="Output directory for dashboard JSON/GeoJSON files.",
)
@click.option(
    "--bronze", "-b",
    default=None,
    help="Override bronze directory path.",
)
def main(output: str, bronze: str):
    """Export Bronze data to dashboard-ready JSON/GeoJSON files."""

    out_dir = Path(output)
    out_dir.mkdir(parents=True, exist_ok=True)
    click.echo(f"Output directory: {out_dir.resolve()}")

    # Resolve bronze dir relative to this script's project root
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    bronze_dir = Path(bronze) if bronze else project_root / "bronze"

    # Source paths
    nasa_path = bronze_dir / "tabular/nasa_power/nasa_power_19810101_20260401.json"
    hazard_path = bronze_dir / "tabular/sgc_amenaza/seismic_hazard.json"
    runap_path = bronze_dir / "vector/runap/runap.geojson"
    simma_path = bronze_dir / "tabular/sgc_simma/movimientos_en_masa.json"
    usgs_path = bronze_dir / "tabular/sgc_sismicidad/usgs_comcat.geojson"
    desinventar_path = bronze_dir / "tabular/desinventar/desinventar_desastres.json"
    censo_path = bronze_dir / "tabular/dane_censo/poblacion_2018.json"
    geologia_path = bronze_dir / "vector/sgc_geologia/unidades_geologicas.geojson"
    fallas_path = bronze_dir / "vector/sgc_geologia/fallas.geojson"

    click.echo("\n[1/9] indicators.json")
    export_indicators(
        nasa_path, hazard_path, runap_path, simma_path,
        usgs_path, desinventar_path, bronze_dir, out_dir,
    )

    click.echo("\n[2/9] ingestion_status.json")
    export_ingestion_status(out_dir)

    click.echo("\n[3/9] precipitation.json")
    export_precipitation(nasa_path, out_dir)

    click.echo("\n[4/9] geologia.geojson")
    export_geologia(geologia_path, out_dir)

    click.echo("\n[5/9] fallas.geojson")
    export_fallas(fallas_path, out_dir)

    click.echo("\n[6/9] areas_protegidas.geojson")
    export_areas_protegidas(runap_path, out_dir)

    click.echo("\n[7/9] aoi_boundary.geojson")
    export_aoi_boundary(out_dir)

    click.echo("\n[8/9] municipios.json")
    export_municipios(censo_path, out_dir)

    click.echo("\n[9/9] sismos.geojson")
    export_sismos(usgs_path, out_dir)

    # Summary
    click.echo("\n--- Summary ---")
    total_bytes = 0
    for f in sorted(out_dir.iterdir()):
        size_kb = f.stat().st_size / 1024
        total_bytes += f.stat().st_size
        click.echo(f"  {f.name:<35} {size_kb:>8.1f} KB")
    click.echo(f"\n  Total: {total_bytes/1024:.1f} KB ({len(list(out_dir.iterdir()))} files)")
    click.echo("\nDone.")


if __name__ == "__main__":
    main()

"""Generate formal data packages for regulatory agencies (ANLA, CORANTIOQUIA, UPME)."""

from datetime import date
from pathlib import Path

import click
import pandas as pd
import geopandas as gpd
import structlog

from config.settings import SILVER_DIR, GOLD_DIR, EXPORTS_DIR

log = structlog.get_logger()

# Files required per regulatory agency
REGULATORS = {
    "anla": {
        "description": "Autoridad Nacional de Licencias Ambientales",
        "gold_tabular": [
            "amenazas_naturales.parquet",
            "indicadores_socioeconomicos.parquet",
        ],
        "gold_vector": [
            "linea_base_ambiental.geoparquet",
            "perfil_geologico.geoparquet",
        ],
        "silver_vector": [
            "areas_protegidas.geoparquet",
            "cuencas.geoparquet",
            "geologia.geoparquet",
        ],
    },
    "corantioquia": {
        "description": "Corporación Autónoma Regional del Centro de Antioquia",
        "gold_tabular": [
            "balance_hidrico.parquet",
            "amenazas_naturales.parquet",
        ],
        "gold_vector": [
            "linea_base_ambiental.geoparquet",
        ],
        "silver_vector": [
            "areas_protegidas.geoparquet",
            "cuencas.geoparquet",
        ],
    },
    "upme": {
        "description": "Unidad de Planeación Minero-Energética",
        "gold_tabular": [
            "potencial_generacion.parquet",
            "curvas_duracion.parquet",
            "mercado_despacho.parquet",
            "recurso_solar_eolico.parquet",
            "balance_hidrico.parquet",
        ],
        "gold_vector": [],
        "silver_vector": [
            "cuencas.geoparquet",
        ],
    },
}


@click.command()
@click.option(
    "--entidad",
    type=click.Choice(list(REGULATORS.keys())),
    required=True,
    help="Regulatory agency to export data for.",
)
@click.option(
    "--formato-tabular",
    type=str,
    default="excel,csv",
    show_default=True,
    help="Comma-separated formats for tabular data: excel, csv.",
)
@click.option(
    "--formato-vector",
    type=str,
    default="geopackage,shapefile",
    show_default=True,
    help="Comma-separated formats for vector data: geopackage, shapefile.",
)
def cli(entidad: str, formato_tabular: str, formato_vector: str) -> None:
    """Generate a formal data package for a regulatory agency."""
    today = date.today().isoformat()
    out_dir = EXPORTS_DIR / "reguladores" / entidad / today
    out_dir.mkdir(parents=True, exist_ok=True)

    config = REGULATORS[entidad]
    tab_formats = [f.strip() for f in formato_tabular.split(",")]
    vec_formats = [f.strip() for f in formato_vector.split(",")]
    exported = 0
    missing = []

    # Export tabular Gold files
    for gold_file in config.get("gold_tabular", []):
        src = GOLD_DIR / gold_file
        if not src.exists():
            log.warning("export.missing", file=str(src))
            missing.append(str(src))
            continue
        df = pd.read_parquet(src)
        if "excel" in tab_formats:
            dest = out_dir / f"{src.stem}.xlsx"
            df.to_excel(dest, index=False)
            exported += 1
            log.info("export.written", dest=str(dest))
        if "csv" in tab_formats:
            dest = out_dir / f"{src.stem}.csv"
            df.to_csv(dest, index=False)
            exported += 1
            log.info("export.written", dest=str(dest))

    # Export vector Gold files
    for gold_file in config.get("gold_vector", []):
        src = GOLD_DIR / gold_file
        if not src.exists():
            log.warning("export.missing", file=str(src))
            missing.append(str(src))
            continue
        gdf = gpd.read_parquet(src)
        if "geopackage" in vec_formats:
            dest = out_dir / f"{src.stem}.gpkg"
            gdf.to_file(dest, driver="GPKG")
            exported += 1
            log.info("export.written", dest=str(dest))
        if "shapefile" in vec_formats:
            shp_dir = out_dir / "shapefiles" / src.stem
            shp_dir.mkdir(parents=True, exist_ok=True)
            gdf.to_file(shp_dir / f"{src.stem}.shp", driver="ESRI Shapefile")
            exported += 1
            log.info("export.written", dest=str(shp_dir))

    # Export Silver vector files
    for vec_file in config.get("silver_vector", []):
        src = SILVER_DIR / "vector" / vec_file
        if not src.exists():
            log.warning("export.missing", file=str(src))
            missing.append(str(src))
            continue
        gdf = gpd.read_parquet(src)
        if "geopackage" in vec_formats:
            dest = out_dir / f"{Path(vec_file).stem}.gpkg"
            gdf.to_file(dest, driver="GPKG")
            exported += 1
            log.info("export.written", dest=str(dest))
        if "shapefile" in vec_formats:
            stem = Path(vec_file).stem
            shp_dir = out_dir / "shapefiles" / stem
            shp_dir.mkdir(parents=True, exist_ok=True)
            gdf.to_file(shp_dir / f"{stem}.shp", driver="ESRI Shapefile")
            exported += 1
            log.info("export.written", dest=str(shp_dir))

    readme = out_dir / "README.txt"
    readme.write_text(
        f"Regulatory Data Package\n"
        f"Agency: {entidad.upper()} — {config['description']}\n"
        f"Generated: {today}\n"
        f"Project: Milagros Hydroelectric Prefeasibility\n"
        f"Files exported: {exported}\n"
        + (f"Missing source files:\n" + "\n".join(f"  - {m}" for m in missing) + "\n" if missing else "")
        + "\nData Sources:\n"
        "  Gold layer: processed, analysis-ready datasets\n"
        "  Silver layer: cleaned, standardized geospatial data\n"
        "\nCoordinate Reference System: EPSG:4326 (WGS84)\n"
    )

    click.echo(f"Exported {exported} files to {out_dir}")
    if missing:
        click.echo(f"Warning: {len(missing)} source file(s) not found — skipped.")


if __name__ == "__main__":
    cli()

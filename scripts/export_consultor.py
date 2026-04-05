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
        "gold": [
            "balance_hidrico.parquet",
            "series_caudal.parquet",
            "curvas_duracion.parquet",
            "potencial_generacion.parquet",
        ],
        "silver_vector": ["cuencas.geoparquet"],
    },
    "geologia": {
        "gold": [
            "perfil_geologico.geoparquet",
            "amenazas_naturales.parquet",
        ],
        "silver_vector": ["geologia.geoparquet", "fallas.geoparquet"],
    },
    "ambiental": {
        "gold": ["linea_base_ambiental.geoparquet"],
        "silver_vector": ["areas_protegidas.geoparquet"],
    },
    "electrico": {
        "gold": [
            "mercado_despacho.parquet",
            "potencial_generacion.parquet",
            "recurso_solar_eolico.parquet",
        ],
    },
}


@click.command()
@click.option(
    "--disciplina",
    type=click.Choice(list(DISCIPLINES.keys())),
    required=True,
    help="Consultant discipline to export data for.",
)
@click.option(
    "--formato",
    type=str,
    default="excel,geopackage",
    show_default=True,
    help="Comma-separated list of output formats: excel, csv, geopackage.",
)
def cli(disciplina: str, formato: str) -> None:
    """Generate a data package for a consultant discipline."""
    today = date.today().isoformat()
    out_dir = EXPORTS_DIR / "consultores" / disciplina / today
    out_dir.mkdir(parents=True, exist_ok=True)

    config = DISCIPLINES[disciplina]
    formats = [f.strip() for f in formato.split(",")]
    exported = 0

    # Export Gold files
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

    # Export Silver vector files
    for vec_file in config.get("silver_vector", []):
        src = SILVER_DIR / "vector" / vec_file
        if not src.exists():
            log.warning("export.missing", file=str(src))
            continue
        if "geopackage" in formats:
            gdf = gpd.read_parquet(src)
            gdf.to_file(out_dir / f"{src.stem}.gpkg", driver="GPKG")
            exported += 1

    readme = out_dir / "README.txt"
    readme.write_text(
        f"Data package: {disciplina}\n"
        f"Generated: {today}\n"
        f"Project: Milagros Hydroelectric Prefeasibility\n"
        f"Files: {exported}\n"
    )

    click.echo(f"Exported {exported} files to {out_dir}")


if __name__ == "__main__":
    cli()

"""CLI orchestrator for Bronze → Silver → Gold processing."""

import click
import structlog

from config.settings import BRONZE_DIR, SILVER_DIR, GOLD_DIR, CATALOG_DB, ensure_dirs
from catalog.manager import CatalogManager

log = structlog.get_logger()

SILVER_PROCESSORS = {
    "hidrologia":        {"module": "processors.tabular.hidrologia",        "function": "process"},
    "mercado_electrico": {"module": "processors.tabular.mercado_electrico",  "function": "process"},
    "socioeconomico":    {"module": "processors.tabular.socioeconomico",     "function": "process"},
    "amenazas":          {"module": "processors.tabular.amenazas",           "function": "process"},
    "era5_raster":       {"module": "processors.raster.era5",                "function": "process"},
    "chirps_raster":     {"module": "processors.raster.chirps",              "function": "process"},
    "dem_raster":        {"module": "processors.raster.dem",                 "function": "process"},
    "mapbiomas_raster":  {"module": "processors.raster.mapbiomas",           "function": "process"},
    "cuencas_vector":    {"module": "processors.vector.cuencas",             "function": "process"},
    "geologia_vector":   {"module": "processors.vector.geologia",            "function": "process"},
    "cobertura_vector":  {"module": "processors.vector.cobertura",           "function": "process"},
    "open_meteo":        {"module": "processors.tabular.open_meteo",         "function": "process"},
    "sentinel2_raster":  {"module": "processors.raster.sentinel2",           "function": "process"},
    "ideam":             {"module": "processors.tabular.ideam",              "function": "process"},
}

GOLD_VIEWS = {
    "balance_hidrico":              {"module": "processors.gold.balance_hidrico",              "function": "build"},
    "series_caudal":                {"module": "processors.gold.series_caudal",                "function": "build"},
    "curvas_duracion":              {"module": "processors.gold.curvas_duracion",              "function": "build"},
    "potencial_generacion":         {"module": "processors.gold.potencial_generacion",         "function": "build"},
    "perfil_geologico":             {"module": "processors.gold.perfil_geologico",             "function": "build"},
    "amenazas_naturales":           {"module": "processors.gold.amenazas_naturales",           "function": "build"},
    "mercado_despacho":             {"module": "processors.gold.mercado_despacho",             "function": "build"},
    "indicadores_socioeconomicos":  {"module": "processors.gold.indicadores_socioeconomicos",  "function": "build"},
    "linea_base_ambiental":         {"module": "processors.gold.linea_base_ambiental",         "function": "build"},
    "recurso_solar_eolico":         {"module": "processors.gold.recurso_solar_eolico",         "function": "build"},
}


def _run_processor(name: str, registry: dict, catalog: CatalogManager) -> None:
    import importlib

    entry = registry[name]
    mod = importlib.import_module(entry["module"])
    func = getattr(mod, entry["function"])
    func(bronze_dir=BRONZE_DIR, silver_dir=SILVER_DIR, gold_dir=GOLD_DIR, catalog=catalog)


@click.command()
@click.option("--layer", type=click.Choice(["silver", "gold"]), required=True,
              help="Which layer to process: silver (Bronze→Silver) or gold (Silver→Gold)")
@click.option("--category", type=str, default=None,
              help="Process a single category/view by name")
@click.option("--dry-run", is_flag=True,
              help="List processors that would run without executing them")
def cli(layer: str, category: str | None, dry_run: bool) -> None:
    """Process data from Bronze → Silver or Silver → Gold."""
    ensure_dirs()
    catalog = CatalogManager(CATALOG_DB)

    registry = SILVER_PROCESSORS if layer == "silver" else GOLD_VIEWS

    if category:
        if category not in registry:
            click.echo(
                f"Unknown: {category}. Available: {', '.join(sorted(registry))}"
            )
            return
        targets = {category: registry[category]}
    else:
        targets = registry

    if dry_run:
        click.echo(f"Would run {len(targets)} {layer} processors:")
        for name in sorted(targets):
            click.echo(f"  {name}")
        return

    click.echo(f"Processing {len(targets)} → {layer}...")
    success, failed = 0, 0

    for name in sorted(targets):
        try:
            _run_processor(name, registry, catalog)
            success += 1
        except Exception as e:
            log.error(f"processor.{layer}.failed", name=name, error=str(e))
            failed += 1

    click.echo(f"Done: {success} succeeded, {failed} failed")
    catalog.close()


if __name__ == "__main__":
    cli()

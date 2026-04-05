"""CLI orchestrator for data ingestion."""

import click
import structlog

from config.settings import BRONZE_DIR, CATALOG_DB, ensure_dirs
from catalog.manager import CatalogManager

log = structlog.get_logger()

INGESTOR_REGISTRY = {
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

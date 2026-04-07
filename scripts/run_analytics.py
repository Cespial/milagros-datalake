"""Orchestrator for engineering analytics modules."""
import click
import structlog
from config.settings import BRONZE_DIR, SILVER_DIR, GOLD_DIR, ensure_dirs

log = structlog.get_logger()

MODULES = {
    "hidrologico": {"module": "analytics.hidrologico", "function": "run"},
    "geotecnico": {"module": "analytics.geotecnico", "function": "run"},
    "eia": {"module": "analytics.eia", "function": "run"},
    "financiero": {"module": "analytics.financiero", "function": "run"},
    "solar": {"module": "analytics.solar", "function": "run"},
}

@click.command()
@click.option("--module", type=str, help="Run a specific module")
@click.option("--dry-run", is_flag=True)
def cli(module, dry_run):
    ensure_dirs()
    targets = {module: MODULES[module]} if module and module in MODULES else MODULES
    if dry_run:
        for name in sorted(targets):
            click.echo(f"  {name}")
        return
    for name, cfg in sorted(targets.items()):
        try:
            import importlib
            mod = importlib.import_module(cfg["module"])
            func = getattr(mod, cfg["function"])
            log.info(f"analytics.{name}.start")
            func(bronze_dir=BRONZE_DIR, silver_dir=SILVER_DIR, gold_dir=GOLD_DIR)
            log.info(f"analytics.{name}.done")
        except Exception as e:
            log.error(f"analytics.{name}.failed", error=str(e))

if __name__ == "__main__":
    cli()

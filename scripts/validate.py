"""Validate data lake integrity: catalog counts, file existence, layer summaries."""

from pathlib import Path

import click
import structlog

from config.settings import BRONZE_DIR, SILVER_DIR, GOLD_DIR, EXPORTS_DIR, CATALOG_DB, CATEGORIES
from catalog.manager import CatalogManager

log = structlog.get_logger()

LAYERS = ["bronze", "silver", "gold"]
LAYER_DIRS = {
    "bronze": BRONZE_DIR,
    "silver": SILVER_DIR,
    "gold": GOLD_DIR,
}


def _check_file_existence(entries: list[dict]) -> tuple[int, list[str]]:
    """Return (missing_count, list_of_missing_paths)."""
    missing = []
    for row in entries:
        fp = row.get("file_path", "")
        if fp and not Path(fp).exists():
            missing.append(fp)
    return len(missing), missing


def _validate_layer(catalog: CatalogManager, layer: str, verbose: bool) -> dict:
    """Validate a single layer and return a summary dict."""
    entries = catalog.list_datasets(layer=layer)
    total = len(entries)
    missing_count, missing_paths = _check_file_existence(entries)

    # Group by category
    by_category: dict[str, int] = {}
    for row in entries:
        cat = row.get("category") or "unknown"
        by_category[cat] = by_category.get(cat, 0) + 1

    result = {
        "layer": layer,
        "total": total,
        "missing_files": missing_count,
        "by_category": by_category,
        "missing_paths": missing_paths,
    }
    return result


def _print_summary(summary: dict, verbose: bool) -> None:
    layer = summary["layer"]
    total = summary["total"]
    missing = summary["missing_files"]
    status = "OK" if missing == 0 else f"WARN ({missing} missing)"

    click.echo(f"\n{'='*50}")
    click.echo(f"  Layer: {layer.upper()}  |  Total entries: {total}  |  {status}")
    click.echo(f"{'='*50}")

    if summary["by_category"]:
        click.echo("  By category:")
        for cat, count in sorted(summary["by_category"].items()):
            click.echo(f"    {cat:<30} {count:>5} entries")
    else:
        click.echo("  (no entries)")

    if verbose and summary["missing_paths"]:
        click.echo("\n  Missing files:")
        for p in summary["missing_paths"]:
            click.echo(f"    ! {p}")


@click.command()
@click.option(
    "--layer",
    type=click.Choice(LAYERS + ["all"]),
    default="all",
    show_default=True,
    help="Layer to validate (bronze, silver, gold, or all).",
)
@click.option(
    "--category",
    type=str,
    default=None,
    help="Filter validation to a specific category.",
)
@click.option(
    "--verbose", "-v",
    is_flag=True,
    default=False,
    help="Print missing file paths.",
)
@click.option(
    "--fail-on-missing",
    is_flag=True,
    default=False,
    help="Exit with code 1 if any registered files are missing.",
)
def cli(layer: str, category: str | None, verbose: bool, fail_on_missing: bool) -> None:
    """Validate data lake integrity across catalog and filesystem."""
    if not CATALOG_DB.exists():
        click.echo(f"Catalog not found at {CATALOG_DB}. Run ingestion first.")
        raise SystemExit(1)

    catalog = CatalogManager(CATALOG_DB)

    layers_to_check = LAYERS if layer == "all" else [layer]
    total_missing = 0
    grand_total = 0

    for lyr in layers_to_check:
        if category:
            entries = catalog.list_datasets(layer=lyr, category=category)
            missing_count, missing_paths = _check_file_existence(entries)
            by_category = {category: len(entries)} if entries else {}
            summary = {
                "layer": lyr,
                "total": len(entries),
                "missing_files": missing_count,
                "by_category": by_category,
                "missing_paths": missing_paths,
            }
        else:
            summary = _validate_layer(catalog, lyr, verbose)

        _print_summary(summary, verbose)
        total_missing += summary["missing_files"]
        grand_total += summary["total"]

    click.echo(f"\n{'='*50}")
    click.echo(f"  TOTAL  |  Entries: {grand_total}  |  Missing files: {total_missing}")
    click.echo(f"{'='*50}\n")

    # Also list entries by category across all layers if requested
    if layer == "all" and not category:
        click.echo("Category coverage across all layers:")
        for cat in sorted(CATEGORIES):
            entries = catalog.list_datasets(category=cat)
            if entries:
                layers_present = sorted({r["layer"] for r in entries})
                click.echo(f"  {cat:<30} {len(entries):>4} entries  [{', '.join(layers_present)}]")

    catalog.close()

    if fail_on_missing and total_missing > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    cli()

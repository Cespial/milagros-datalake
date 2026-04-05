"""Generate executive summary data packages for investors."""

from datetime import date
from pathlib import Path

import click
import pandas as pd
import structlog

from config.settings import GOLD_DIR, EXPORTS_DIR

log = structlog.get_logger()

# Key Gold files for the executive investor summary
INVESTOR_FILES = [
    "potencial_generacion.parquet",
    "curvas_duracion.parquet",
    "mercado_despacho.parquet",
    "indicadores_socioeconomicos.parquet",
    "recurso_solar_eolico.parquet",
    "amenazas_naturales.parquet",
]


@click.command()
@click.option(
    "--formato",
    type=str,
    default="excel",
    show_default=True,
    help="Comma-separated list of output formats: excel, csv.",
)
def cli(formato: str) -> None:
    """Generate an executive summary data package for investors."""
    today = date.today().isoformat()
    out_dir = EXPORTS_DIR / "inversionistas" / today
    out_dir.mkdir(parents=True, exist_ok=True)

    formats = [f.strip() for f in formato.split(",")]
    exported = 0
    missing = []

    for gold_file in INVESTOR_FILES:
        src = GOLD_DIR / gold_file
        if not src.exists():
            log.warning("export.missing", file=str(src))
            missing.append(gold_file)
            continue

        df = pd.read_parquet(src)

        if "excel" in formats:
            dest = out_dir / f"{src.stem}.xlsx"
            df.to_excel(dest, index=False)
            exported += 1
            log.info("export.written", dest=str(dest), rows=len(df))

        if "csv" in formats:
            dest = out_dir / f"{src.stem}.csv"
            df.to_csv(dest, index=False)
            exported += 1
            log.info("export.written", dest=str(dest), rows=len(df))

    # Write combined Excel workbook with all datasets as separate sheets
    if "excel" in formats and exported > 0:
        combined_path = out_dir / "resumen_ejecutivo.xlsx"
        with pd.ExcelWriter(combined_path, engine="openpyxl") as writer:
            for gold_file in INVESTOR_FILES:
                src = GOLD_DIR / gold_file
                if not src.exists():
                    continue
                df = pd.read_parquet(src)
                sheet_name = src.stem[:31]  # Excel sheet name max 31 chars
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        log.info("export.combined_written", dest=str(combined_path))

    readme = out_dir / "README.txt"
    readme.write_text(
        f"Investor Executive Summary Package\n"
        f"Generated: {today}\n"
        f"Project: Milagros Hydroelectric Prefeasibility\n"
        f"Files exported: {exported}\n"
        + (f"Missing source files: {', '.join(missing)}\n" if missing else "")
    )

    click.echo(f"Exported {exported} files to {out_dir}")
    if missing:
        click.echo(f"Warning: {len(missing)} source file(s) not found — skipped.")


if __name__ == "__main__":
    cli()

"""Gold view: Environmental baseline — Corine + RUNAP + CORANTIOQUIA.

Reads silver/vector/cobertura/ (corine_lc, runap, corantioquia) and
merges into a unified environmental baseline GeoParquet.
Output: gold/linea_base_ambiental.geoparquet.
"""

from pathlib import Path

import structlog

from catalog.manager import CatalogManager

log = structlog.get_logger()

OUT_FILE = "linea_base_ambiental.geoparquet"

SILVER_SOURCES = [
    {
        "file": "vector/cobertura/corine_lc.parquet",
        "layer_name": "corine_lc",
        "source_name": "Corine Land Cover",
        "category": "biodiversidad",
    },
    {
        "file": "vector/cobertura/runap.parquet",
        "layer_name": "runap",
        "source_name": "RUNAP Protected Areas",
        "category": "biodiversidad",
    },
    {
        "file": "vector/cobertura/corantioquia.parquet",
        "layer_name": "corantioquia",
        "source_name": "CORANTIOQUIA",
        "category": "regulatorio",
    },
]


def _write_empty(out_path: Path, catalog: CatalogManager, notes: str = "") -> None:
    """Write empty GeoParquet and register as empty in catalog."""
    import geopandas as gpd
    import pandas as pd

    empty_gdf = gpd.GeoDataFrame(
        {"_layer": pd.Series([], dtype="object")},
        geometry=gpd.GeoSeries([], crs="EPSG:4326"),
    )
    empty_gdf.set_crs("EPSG:4326", inplace=True)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    empty_gdf.to_parquet(out_path)

    catalog.register({
        "dataset_id": "linea_base_ambiental",
        "source": "Corine LC / RUNAP / CORANTIOQUIA",
        "category": "biodiversidad",
        "data_type": "vector",
        "layer": "gold",
        "file_path": str(out_path),
        "format": "geoparquet",
        "crs": "EPSG:4326",
        "ingestor": "processor.gold.linea_base_ambiental",
        "status": "empty",
        "notes": notes,
    })


def build(bronze_dir: Path, silver_dir: Path, gold_dir: Path, catalog: CatalogManager, **kwargs):
    """Build environmental baseline gold view from silver vector cobertura layers."""
    import geopandas as gpd
    import pandas as pd

    out_path = gold_dir / OUT_FILE
    gold_dir.mkdir(parents=True, exist_ok=True)

    frames = []
    loaded_sources = []

    for spec in SILVER_SOURCES:
        src_path = silver_dir / spec["file"]
        if not src_path.exists():
            log.warning("linea_base_ambiental.missing_layer", file=spec["file"])
            continue

        try:
            gdf = gpd.read_parquet(src_path)
            if gdf.empty:
                log.warning("linea_base_ambiental.empty_layer", layer=spec["layer_name"])
                continue
            gdf["_layer"] = spec["layer_name"]
            gdf["_source"] = spec["source_name"]
            gdf["_category"] = spec["category"]
            frames.append(gdf)
            loaded_sources.append(spec["layer_name"])
            log.info("linea_base_ambiental.loaded", layer=spec["layer_name"], rows=len(gdf))
        except Exception as exc:
            log.warning("linea_base_ambiental.read_failed", file=spec["file"], error=str(exc))

    if not frames:
        log.warning("linea_base_ambiental.no_layers")
        _write_empty(out_path, catalog, notes="No silver cobertura layers available yet")
        return

    result = pd.concat(frames, ignore_index=True)
    result = gpd.GeoDataFrame(result, crs=frames[0].crs)

    # Ensure WGS84
    if result.crs is not None and str(result.crs) != "EPSG:4326":
        result = result.to_crs("EPSG:4326")
    elif result.crs is None:
        result = result.set_crs("EPSG:4326")

    result.to_parquet(out_path)
    log.info("linea_base_ambiental.done", rows=len(result), layers=loaded_sources)

    catalog.register({
        "dataset_id": "linea_base_ambiental",
        "source": "Corine LC / RUNAP / CORANTIOQUIA",
        "category": "biodiversidad",
        "data_type": "vector",
        "layer": "gold",
        "file_path": str(out_path),
        "format": "geoparquet",
        "crs": "EPSG:4326",
        "ingestor": "processor.gold.linea_base_ambiental",
        "status": "complete",
        "notes": f"Merged layers: {', '.join(loaded_sources)} ({len(result)} features total)",
    })

"""Gold view: Geological profile — combined geology + faults.

Reads silver/vector/geologia/*.parquet (SGC geology, IGAC cartography)
and silver/vector/geologia/fallas.parquet if present, merges them,
and outputs a unified GeoParquet at gold/perfil_geologico.geoparquet.
"""

from pathlib import Path

import structlog

from catalog.manager import CatalogManager

log = structlog.get_logger()

OUT_FILE = "perfil_geologico.geoparquet"


def build(bronze_dir: Path, silver_dir: Path, gold_dir: Path, catalog: CatalogManager, **kwargs):
    """Build geological profile gold view from silver vector geology layers."""
    import geopandas as gpd
    import pandas as pd

    out_path = gold_dir / OUT_FILE
    gold_dir.mkdir(parents=True, exist_ok=True)

    geologia_dir = silver_dir / "vector" / "geologia"

    if not geologia_dir.exists():
        log.warning("perfil_geologico.no_silver", path=str(geologia_dir))
        # Write empty GeoParquet
        _write_empty(out_path, catalog, notes="Silver geology directory not yet available")
        return

    # Collect all parquet/geoparquet files in vector/geologia
    parquet_files = sorted(geologia_dir.glob("*.parquet"))
    if not parquet_files:
        log.warning("perfil_geologico.no_parquet", path=str(geologia_dir))
        _write_empty(out_path, catalog, notes="No parquet files in silver/vector/geologia/")
        return

    frames = []
    for pf in parquet_files:
        try:
            gdf = gpd.read_parquet(pf)
            if not gdf.empty:
                gdf["_source_layer"] = pf.stem
                frames.append(gdf)
                log.info("perfil_geologico.read", file=pf.name, rows=len(gdf))
        except Exception as exc:
            log.warning("perfil_geologico.read_failed", file=str(pf), error=str(exc))

    if not frames:
        log.warning("perfil_geologico.all_empty")
        _write_empty(out_path, catalog, notes="All geology layers empty after read")
        return

    result = pd.concat(frames, ignore_index=True)
    result = gpd.GeoDataFrame(result, crs=frames[0].crs)

    # Ensure WGS84
    if result.crs is not None and str(result.crs) != "EPSG:4326":
        result = result.to_crs("EPSG:4326")
    elif result.crs is None:
        result = result.set_crs("EPSG:4326")

    result.to_parquet(out_path)
    log.info("perfil_geologico.done", rows=len(result), path=str(out_path))

    catalog.register({
        "dataset_id": "perfil_geologico",
        "source": "SGC / IGAC",
        "category": "geologia",
        "data_type": "vector",
        "layer": "gold",
        "file_path": str(out_path),
        "format": "geoparquet",
        "crs": "EPSG:4326",
        "ingestor": "processor.gold.perfil_geologico",
        "status": "complete",
        "notes": f"Merged {len(frames)} geology layers from silver/vector/geologia/",
    })


def _write_empty(out_path: Path, catalog: CatalogManager, notes: str = "") -> None:
    """Write empty GeoParquet and register as empty in catalog."""
    import geopandas as gpd
    from shapely.geometry import Point

    empty_gdf = gpd.GeoDataFrame(
        {"_source_layer": pd.Series([], dtype=str)},
        geometry=gpd.GeoSeries([], crs="EPSG:4326"),
        crs="EPSG:4326",
    )

    import pandas as pd
    empty_gdf = gpd.GeoDataFrame(
        {"_source_layer": pd.Series([], dtype="object")},
        geometry=gpd.GeoSeries([], crs="EPSG:4326"),
    )
    empty_gdf.set_crs("EPSG:4326", inplace=True)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    empty_gdf.to_parquet(out_path)

    catalog.register({
        "dataset_id": "perfil_geologico",
        "source": "SGC / IGAC",
        "category": "geologia",
        "data_type": "vector",
        "layer": "gold",
        "file_path": str(out_path),
        "format": "geoparquet",
        "crs": "EPSG:4326",
        "ingestor": "processor.gold.perfil_geologico",
        "status": "empty",
        "notes": notes,
    })

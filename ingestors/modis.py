"""MODIS LST, NDVI, and fire composites via GEE.

Collections:
  MODIS/061/MOD11A2  — Land Surface Temperature 8-day composite at 1 km
  MODIS/061/MOD13A2  — NDVI 16-day composite at 1 km
  MODIS/061/MOD14A2  — Fire/thermal anomalies 8-day at 1 km

Downloads annual composites (2000-2025):
  - LST: mean of daytime LST (LST_Day_1km), scaled to °C
  - NDVI: max NDVI (NDVI band), scaled to real value
  - Fire: pixel count where FireMask >= 7 (confirmed fire)
"""

import os
from pathlib import Path

import ee
import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

DOWNLOAD_SCALE = 1000  # MODIS native 1 km
START_YEAR = 2000
END_YEAR = 2025


class ModisIngestor(BaseIngestor):
    name = "modis"
    source_type = "gee"
    data_type = "raster"
    category = "teledeteccion"
    schedule = "annual"
    license = "NASA Open Data (MODIS)"

    def _download_tif(self, image: ee.Image, aoi: ee.Geometry, band: str) -> bytes:
        url = image.select(band).getDownloadURL({
            "scale": DOWNLOAD_SCALE,
            "region": aoi,
            "format": "GEO_TIFF",
            "crs": "EPSG:4326",
        })
        resp = httpx.get(url, timeout=300, follow_redirects=True)
        resp.raise_for_status()
        return resp.content

    def fetch(self, **kwargs) -> list[Path]:
        ee.Initialize(project=os.environ.get("GEE_PROJECT"))

        aoi = ee.Geometry.BBox(
            AOI_BBOX["west"], AOI_BBOX["south"],
            AOI_BBOX["east"], AOI_BBOX["north"],
        )

        start_year = kwargs.get("start_year", START_YEAR)
        end_year = kwargs.get("end_year", END_YEAR)
        paths = []

        for year in range(start_year, end_year + 1):
            lst_path = self.bronze_dir / f"lst_{year}.tif"
            ndvi_path = self.bronze_dir / f"ndvi_{year}.tif"
            fire_path = self.bronze_dir / f"fire_{year}.tif"

            # ── LST ────────────────────────────────────────────────────────────
            if lst_path.exists():
                log.info("modis.lst.skip_existing", year=year)
            else:
                try:
                    col = (
                        ee.ImageCollection("MODIS/061/MOD11A2")
                        .filterBounds(aoi)
                        .filterDate(f"{year}-01-01", f"{year}-12-31")
                        .select("LST_Day_1km")
                    )
                    count = col.size().getInfo()
                    if count == 0:
                        log.warning("modis.lst.no_images", year=year)
                    else:
                        log.info("modis.lst.processing", year=year, count=count)
                        # Scale factor: 0.02 to convert DN to Kelvin, then − 273.15 for °C
                        mean_lst = (
                            col.mean()
                            .multiply(0.02)
                            .subtract(273.15)
                            .rename("LST_C")
                            .clip(aoi)
                        )
                        data = self._download_tif(mean_lst, aoi, "LST_C")
                        lst_path.write_bytes(data)
                        log.info("modis.lst.saved", year=year, size_mb=round(len(data) / 1e6, 2))
                except Exception as exc:
                    log.warning("modis.lst.failed", year=year, error=str(exc))

            # ── NDVI ───────────────────────────────────────────────────────────
            if ndvi_path.exists():
                log.info("modis.ndvi.skip_existing", year=year)
            else:
                try:
                    col = (
                        ee.ImageCollection("MODIS/061/MOD13A2")
                        .filterBounds(aoi)
                        .filterDate(f"{year}-01-01", f"{year}-12-31")
                        .select("NDVI")
                    )
                    count = col.size().getInfo()
                    if count == 0:
                        log.warning("modis.ndvi.no_images", year=year)
                    else:
                        log.info("modis.ndvi.processing", year=year, count=count)
                        max_ndvi = (
                            col.max()
                            .multiply(0.0001)
                            .rename("NDVI")
                            .clip(aoi)
                        )
                        data = self._download_tif(max_ndvi, aoi, "NDVI")
                        ndvi_path.write_bytes(data)
                        log.info("modis.ndvi.saved", year=year, size_mb=round(len(data) / 1e6, 2))
                except Exception as exc:
                    log.warning("modis.ndvi.failed", year=year, error=str(exc))

            # ── Fire ───────────────────────────────────────────────────────────
            if fire_path.exists():
                log.info("modis.fire.skip_existing", year=year)
            else:
                try:
                    col = (
                        ee.ImageCollection("MODIS/061/MOD14A2")
                        .filterBounds(aoi)
                        .filterDate(f"{year}-01-01", f"{year}-12-31")
                        .select("FireMask")
                    )
                    count = col.size().getInfo()
                    if count == 0:
                        log.warning("modis.fire.no_images", year=year)
                    else:
                        log.info("modis.fire.processing", year=year, count=count)
                        # Count pixels where FireMask >= 7 (confirmed fire)
                        fire_count = (
                            col.map(lambda img: img.gte(7).rename("fire"))
                            .sum()
                            .clip(aoi)
                        )
                        data = self._download_tif(fire_count, aoi, "fire")
                        fire_path.write_bytes(data)
                        log.info("modis.fire.saved", year=year, size_mb=round(len(data) / 1e6, 2))
                except Exception as exc:
                    log.warning("modis.fire.failed", year=year, error=str(exc))

            for p in [lst_path, ndvi_path, fire_path]:
                if p.exists():
                    paths.append(p)

        return paths

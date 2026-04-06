"""Landsat 8/9 (2013-2025) and Landsat 5/7 (1984-2012) NDVI annual composites via GEE."""

import os
from pathlib import Path

import ee
import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

# Landsat surface reflectance scaling factors (Collection 2 Level-2)
# DN → reflectance: multiply by 0.0000275 and subtract 0.2
_SR_SCALE = 0.0000275
_SR_OFFSET = -0.2

# GEE download scale (metres).  Landsat native resolution is 30 m.
# AOI ~55 km E-W x 50 km N-S → 1833 x 1667 px @ 30 m ≈ 12 MB/band (float32).
# Well under GEE's 50 MB per-band limit, so use native 30 m.
# If a download ever fails with a size error fall back to 60 m.
DOWNLOAD_SCALE = 30


def _mask_l8l9_clouds(image: ee.Image) -> ee.Image:
    """Cloud and shadow mask for Landsat 8/9 C2 L2 using QA_PIXEL band."""
    qa = image.select("QA_PIXEL")
    # Bit 3: cloud shadow, Bit 4: cloud
    cloud_shadow_bit = 1 << 3
    cloud_bit = 1 << 4
    mask = (
        qa.bitwiseAnd(cloud_shadow_bit).eq(0)
        .And(qa.bitwiseAnd(cloud_bit).eq(0))
    )
    return image.updateMask(mask)


def _mask_l57_clouds(image: ee.Image) -> ee.Image:
    """Cloud and shadow mask for Landsat 5/7 C2 L2 using QA_PIXEL band."""
    qa = image.select("QA_PIXEL")
    cloud_shadow_bit = 1 << 3
    cloud_bit = 1 << 4
    mask = (
        qa.bitwiseAnd(cloud_shadow_bit).eq(0)
        .And(qa.bitwiseAnd(cloud_bit).eq(0))
    )
    return image.updateMask(mask)


def _scale_l8l9(image: ee.Image) -> ee.Image:
    """Apply C2 L2 scaling to optical SR bands for Landsat 8/9."""
    optical = image.select("SR_B.").multiply(_SR_SCALE).add(_SR_OFFSET)
    return image.addBands(optical, overwrite=True)


def _scale_l57(image: ee.Image) -> ee.Image:
    """Apply C2 L2 scaling to optical SR bands for Landsat 5/7."""
    optical = image.select("SR_B.").multiply(_SR_SCALE).add(_SR_OFFSET)
    return image.addBands(optical, overwrite=True)


class GeeLandsatIngestor(BaseIngestor):
    name = "gee_landsat"
    source_type = "gee"
    data_type = "raster"
    category = "teledeteccion"
    schedule = "annual"
    license = "USGS Public Domain"

    def _collection_for_year(self, year: int) -> tuple[str, str, str]:
        """Return (collection_id, nir_band, red_band) for the given year."""
        if year >= 2022:
            return "LANDSAT/LC09/C02/T1_L2", "SR_B5", "SR_B4"
        elif year >= 2013:
            return "LANDSAT/LC08/C02/T1_L2", "SR_B5", "SR_B4"
        else:
            # 1984-2012: prefer Landsat 7, fall back to Landsat 5
            # We merge both collections and let GEE pick from whatever is available
            return None, "SR_B4", "SR_B3"  # None signals multi-collection merge

    def _build_collection(
        self, year: int, aoi: ee.Geometry
    ) -> tuple[ee.ImageCollection, str, str]:
        """Build a filtered, cloud-masked, scaled collection for the year."""
        start = f"{year}-01-01"
        end = f"{year}-12-31"

        col_id, nir_band, red_band = self._collection_for_year(year)

        if col_id is not None:
            # Single modern collection (L8 or L9)
            col = (
                ee.ImageCollection(col_id)
                .filterBounds(aoi)
                .filterDate(start, end)
                .filter(ee.Filter.lt("CLOUD_COVER", 30))
                .map(_mask_l8l9_clouds)
                .map(_scale_l8l9)
            )
        else:
            # Historical: merge Landsat 7 + Landsat 5
            l7 = (
                ee.ImageCollection("LANDSAT/LE07/C02/T1_L2")
                .filterBounds(aoi)
                .filterDate(start, end)
                .filter(ee.Filter.lt("CLOUD_COVER", 30))
                .map(_mask_l57_clouds)
                .map(_scale_l57)
            )
            l5 = (
                ee.ImageCollection("LANDSAT/LT05/C02/T1_L2")
                .filterBounds(aoi)
                .filterDate(start, end)
                .filter(ee.Filter.lt("CLOUD_COVER", 30))
                .map(_mask_l57_clouds)
                .map(_scale_l57)
            )
            col = l7.merge(l5)

        return col, nir_band, red_band

    def _download_tif(self, image: ee.Image, aoi: ee.Geometry, scale: int) -> bytes:
        """Fetch a single-band GeoTIFF from GEE and return raw bytes."""
        url = image.getDownloadURL({
            "scale": scale,
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

        start_year = kwargs.get("start_year", 1984)
        end_year = kwargs.get("end_year", 2025)
        paths = []

        for year in range(start_year, end_year + 1):
            ndvi_path = self.bronze_dir / f"ndvi_{year}.tif"

            if ndvi_path.exists():
                log.info("gee_landsat.skip_existing", year=year)
                paths.append(ndvi_path)
                continue

            log.info("gee_landsat.processing", year=year)

            col, nir_band, red_band = self._build_collection(year, aoi)

            count = col.size().getInfo()
            if count == 0:
                log.warning("gee_landsat.no_images", year=year)
                continue

            log.info("gee_landsat.image_count", year=year, count=count)

            median = col.median().clip(aoi)

            # NDVI = (NIR - Red) / (NIR + Red)
            ndvi = median.normalizedDifference([nir_band, red_band]).rename("NDVI")

            # Try at native 30 m; if the response looks too large retry at 60 m
            scale = DOWNLOAD_SCALE
            try:
                data = self._download_tif(ndvi, aoi, scale)
                size_mb = len(data) / 1e6
                if size_mb > 45:
                    log.warning("gee_landsat.large_file_retry", year=year, size_mb=round(size_mb, 1))
                    scale = 60
                    data = self._download_tif(ndvi, aoi, scale)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code in (400, 429):
                    log.warning("gee_landsat.scale_fallback", year=year, status=exc.response.status_code)
                    scale = 60
                    data = self._download_tif(ndvi, aoi, scale)
                else:
                    raise

            ndvi_path.write_bytes(data)
            log.info(
                "gee_landsat.ndvi_saved",
                year=year,
                scale=scale,
                size_mb=round(len(data) / 1e6, 1),
            )
            paths.append(ndvi_path)

        return paths

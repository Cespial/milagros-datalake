"""Sentinel-1 SAR backscatter composites via GEE.

Downloads annual median SAR backscatter composites from Sentinel-1 GRD
for VV and VH polarization bands.  Uses Interferometric Wide (IW) swath
mode and descending orbit for consistent geometry over the study area.

Applications:
- Flood detection via VV backscatter ratio changes
- Soil moisture estimation
- InSAR deformation analysis preprocessing
- All-weather monitoring (SAR penetrates clouds — critical in Antioquia)

Collection : COPERNICUS/S1_GRD
Native res : 10 m
Download   : 20 m (keeps each band well under GEE's 50 MB getDownloadURL limit)
AOI        : ~55 km E-W × 50 km N-S  →  ~2 750 × 2 500 px @ 20 m ≈ 27 MB/band
Coverage   : 2015–2025 (Sentinel-1A launch: April 2014; data over Colombia from ~2015)
"""

import os
from pathlib import Path

import ee
import httpx
import structlog

from config.settings import AOI_BBOX
from ingestors.base import BaseIngestor

log = structlog.get_logger()

# Download resolution — 20 m keeps each single-band GeoTIFF well under 50 MB.
# AOI is ~61 km E-W × 50 km N-S  →  3 042 × 2 505 px @ 20 m.
# Sentinel-1 GRD is stored as float64 in GEE (~61 MB per band @ 20 m), which
# exceeds GEE's 50 MB getDownloadURL limit.  Casting to float32 halves it to
# ~30 MB — acceptable and consistent with Sentinel-2 output precision.
DOWNLOAD_SCALE = 20

# Sentinel-1 first reliable annual coverage over northern Antioquia
S1_START_YEAR = 2015
S1_END_YEAR = 2025


class GeeSentinel1Ingestor(BaseIngestor):
    """Annual median SAR backscatter (VV + VH) from Sentinel-1 GRD via GEE."""

    name = "gee_sentinel1"
    source_type = "gee"
    data_type = "raster"
    category = "teledeteccion"
    schedule = "annual"
    license = "Copernicus Open Access"

    def fetch(self, **kwargs) -> list[Path]:
        """Download annual VV and VH GeoTIFFs to bronze_dir.

        Keyword args (all optional):
            start_year (int): first year to fetch, default 2015
            end_year   (int): last year to fetch  (inclusive), default 2025
        """
        ee.Initialize(project=os.environ.get("GEE_PROJECT"))

        aoi = ee.Geometry.BBox(
            AOI_BBOX["west"], AOI_BBOX["south"],
            AOI_BBOX["east"], AOI_BBOX["north"],
        )

        start_year = kwargs.get("start_year", S1_START_YEAR)
        end_year = kwargs.get("end_year", S1_END_YEAR)
        paths: list[Path] = []

        for year in range(start_year, end_year + 1):
            vv_path = self.bronze_dir / f"vv_{year}.tif"
            vh_path = self.bronze_dir / f"vh_{year}.tif"

            if vv_path.exists() and vh_path.exists():
                log.info("gee_sentinel1.skip_existing", year=year)
                paths.extend([vv_path, vh_path])
                continue

            log.info("gee_sentinel1.processing", year=year)

            # Filter to IW mode, dual-pol VV+VH, descending orbit
            col = (
                ee.ImageCollection("COPERNICUS/S1_GRD")
                .filterBounds(aoi)
                .filterDate(f"{year}-01-01", f"{year}-12-31")
                .filter(ee.Filter.eq("instrumentMode", "IW"))
                .filter(ee.Filter.listContains("transmitterReceiverPolarisation", "VV"))
                .filter(ee.Filter.listContains("transmitterReceiverPolarisation", "VH"))
                .filter(ee.Filter.eq("orbitProperties_pass", "DESCENDING"))
                .select(["VV", "VH"])
            )

            count = col.size().getInfo()
            if count == 0:
                log.warning("gee_sentinel1.no_images", year=year)
                continue

            log.info("gee_sentinel1.image_count", year=year, count=count)

            # Annual median composite clipped to AOI
            median = col.median().clip(aoi)

            # GEE thumbnail endpoint computes on demand; allow 10 min for
            # server-side processing + transfer of a ~30 MB float32 GeoTIFF.
            gee_timeout = httpx.Timeout(connect=30, read=600, write=30, pool=30)

            # --- VV band ---
            if not vv_path.exists():
                # Cast float64 → float32 to stay under GEE's 50 MB per-band limit
                vv_img = median.select("VV").toFloat()
                url = vv_img.getDownloadURL({
                    "scale": DOWNLOAD_SCALE,
                    "region": aoi,
                    "format": "GEO_TIFF",
                    "crs": "EPSG:4326",
                })
                log.info("gee_sentinel1.vv_downloading", year=year)
                resp = httpx.get(url, timeout=gee_timeout, follow_redirects=True)
                resp.raise_for_status()
                vv_path.write_bytes(resp.content)
                log.info(
                    "gee_sentinel1.vv_saved",
                    year=year,
                    size_mb=round(len(resp.content) / 1e6, 1),
                )
            else:
                log.info("gee_sentinel1.vv_already_exists", year=year)

            # --- VH band ---
            if not vh_path.exists():
                # Cast float64 → float32 to stay under GEE's 50 MB per-band limit
                vh_img = median.select("VH").toFloat()
                url = vh_img.getDownloadURL({
                    "scale": DOWNLOAD_SCALE,
                    "region": aoi,
                    "format": "GEO_TIFF",
                    "crs": "EPSG:4326",
                })
                log.info("gee_sentinel1.vh_downloading", year=year)
                resp = httpx.get(url, timeout=gee_timeout, follow_redirects=True)
                resp.raise_for_status()
                vh_path.write_bytes(resp.content)
                log.info(
                    "gee_sentinel1.vh_saved",
                    year=year,
                    size_mb=round(len(resp.content) / 1e6, 1),
                )
            else:
                log.info("gee_sentinel1.vh_already_exists", year=year)

            paths.extend([vv_path, vh_path])

        return paths

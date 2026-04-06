# Overnight Processing Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Process all ingested data through Silver→Gold pipeline, fix broken ingestors (MapBiomas, GloFAS, IDEAM, XM), fix the dashboard map, refresh dashboard data, and redeploy.

**Architecture:** Sequential execution — fix code first, then run pipelines, then export and deploy. All tasks run in the milagros-datalake project except Task 6 (dashboard).

**Tech Stack:** Python 3.11, DuckDB, GEE, CDS API, Next.js 16, Vercel

---

## Current State

**Data in Bronze (903 MB):**
- CHIRPS: 46 years (1981-2026) - COMPLETE
- ERA5-Land: 77 years (1950-2026) - COMPLETE
- DEMs: 3 files (Copernicus + SRTM + ALOS) - COMPLETE
- NASA POWER: 45 years - COMPLETE
- SGC: sismicidad (2183), SIMMA (42), geologia (32+17), amenaza (10) - COMPLETE
- DANE: censo (10110) - COMPLETE
- AGRONET: 1.4M records - COMPLETE
- UNGRD: 2718 emergencies - COMPLETE
- RUNAP: 21 protected areas - COMPLETE

**Broken (need fix):**
- MapBiomas: GEE asset path changed
- GloFAS: CDS dataset name changed
- IDEAM DHIME: not attempted yet
- XM SiMEM: not attempted yet
- Dashboard map: not rendering (SSR/hydration issue)

---

### Task 1: Fix MapBiomas GEE Asset Path

**Files:**
- Modify: `ingestors/mapbiomas.py`

The MapBiomas GEE asset `projects/mapbiomas_af_trinacional/public/collection1/mapbiomas_colombia_collection1_integration_v1` no longer exists. Need to find the current asset.

- [ ] **Step 1: Search for the correct MapBiomas asset**

Run:
```bash
python3 -c "
import ee
ee.Initialize(project='ee-maestria-tesis')
# Try the new collection path
assets_to_try = [
    'projects/mapbiomas-public/assets/colombia/collection1/mapbiomas_colombia_collection1_integration_v1',
    'projects/mapbiomas_af_trinacional/public/collection2/mapbiomas_colombia_collection2_integration_v1',
    'projects/mapbiomas-public/assets/colombia/collection2/mapbiomas_colombia_collection2_integration_v1',
    'projects/mapbiomas-colombia/public/collection1/mapbiomas_colombia_collection1_integration_v1',
]
for asset in assets_to_try:
    try:
        img = ee.Image(asset)
        bands = img.bandNames().size().getInfo()
        print(f'FOUND: {asset} ({bands} bands)')
        break
    except Exception as e:
        print(f'NOT FOUND: {asset} - {str(e)[:80]}')
"
```

- [ ] **Step 2: Update the asset path in mapbiomas.py**

Update `MAPBIOMAS_ASSET` with the correct path found in Step 1.

- [ ] **Step 3: Run MapBiomas ingestor**

```bash
cd /Users/cristianespinal/milagros-datalake
PYTHONPATH=. python scripts/ingest_all.py --source mapbiomas
```

- [ ] **Step 4: Commit**

```bash
git add ingestors/mapbiomas.py
git commit -m "fix: update MapBiomas GEE asset path"
```

---

### Task 2: Fix GloFAS CDS Dataset Name

**Files:**
- Modify: `ingestors/glofas.py`

The CDS dataset `cems-glofas-historical` returned 404. The CDS API v2 changed dataset names.

- [ ] **Step 1: Search for the correct GloFAS dataset**

Run:
```bash
python3 -c "
import cdsapi
c = cdsapi.Client()
# Try the new dataset names
datasets = [
    'cems-glofas-historical',
    'cems-glofas-reforecast', 
    'derived-near-surface-meteorological-variables',
]
for ds in datasets:
    try:
        # Just check if dataset exists by requesting metadata
        print(f'Trying: {ds}')
    except:
        pass
"
```

Also check the CDS catalog at https://cds.climate.copernicus.eu/datasets for GloFAS.

- [ ] **Step 2: Update glofas.py with the correct dataset name and request parameters**

- [ ] **Step 3: Test with a single year**

```bash
PYTHONPATH=. python -c "
from ingestors.glofas import GlofasIngestor
from catalog.manager import CatalogManager
from config.settings import BRONZE_DIR, CATALOG_DB
catalog = CatalogManager(CATALOG_DB)
ing = GlofasIngestor(catalog=catalog, bronze_root=BRONZE_DIR)
paths = ing.fetch(start_year=2023, end_year=2023)
print(f'Downloaded: {paths}')
catalog.close()
"
```

- [ ] **Step 4: If successful, run full download**

```bash
PYTHONPATH=. python scripts/ingest_all.py --source glofas
```

- [ ] **Step 5: Commit**

```bash
git add ingestors/glofas.py
git commit -m "fix: update GloFAS CDS dataset name for API v2"
```

---

### Task 3: Run IDEAM DHIME and XM SiMEM

**Files:** None to modify (just run existing ingestors)

- [ ] **Step 1: Run IDEAM DHIME**

```bash
cd /Users/cristianespinal/milagros-datalake
PYTHONPATH=. python scripts/ingest_all.py --source ideam_dhime
```

Check if dataset IDs work. If 404, search for correct IDs on datos.gov.co for IDEAM hydrology data.

- [ ] **Step 2: Run XM SiMEM**

```bash
PYTHONPATH=. python scripts/ingest_all.py --source xm_simem
```

This uses pydataxm which should work without API key.

- [ ] **Step 3: Run HydroSHEDS download**

```bash
PYTHONPATH=. python scripts/ingest_all.py --source hydrosheds
```

Large download (~500MB for South America shapefiles).

- [ ] **Step 4: Report results**

---

### Task 4: Process Bronze → Silver

**Files:** None to modify

- [ ] **Step 1: Run all Silver processors**

```bash
cd /Users/cristianespinal/milagros-datalake
PYTHONPATH=. python scripts/process_all.py --layer silver
```

This runs 11 processors: hidrologia, mercado_electrico, socioeconomico, amenazas, era5_raster, chirps_raster, dem_raster, mapbiomas_raster, cuencas_vector, geologia_vector, cobertura_vector.

Some may fail if their Bronze data is missing — that's OK, they'll log warnings and skip.

- [ ] **Step 2: Check results**

```bash
find /Users/cristianespinal/milagros-datalake/silver -type f | wc -l
du -sh /Users/cristianespinal/milagros-datalake/silver/
```

- [ ] **Step 3: Commit any processor fixes needed**

---

### Task 5: Process Silver → Gold

**Files:** None to modify

- [ ] **Step 1: Run all Gold view builders**

```bash
cd /Users/cristianespinal/milagros-datalake
PYTHONPATH=. python scripts/process_all.py --layer gold
```

This builds 10 Gold views: balance_hidrico, series_caudal, curvas_duracion, potencial_generacion, perfil_geologico, amenazas_naturales, mercado_despacho, indicadores_socioeconomicos, linea_base_ambiental, recurso_solar_eolico.

- [ ] **Step 2: Check Gold output**

```bash
ls -la /Users/cristianespinal/milagros-datalake/gold/
```

- [ ] **Step 3: Validate the data lake**

```bash
PYTHONPATH=. python scripts/validate.py --layer all
```

---

### Task 6: Fix Dashboard Map

**Files:**
- Modify: `milagros-dashboard/src/components/hero-map.tsx`
- Possibly modify: `milagros-dashboard/src/components/hero-map-loader.tsx`

The map doesn't render in production. Root cause investigation needed.

- [ ] **Step 1: Start dev server and test locally**

```bash
cd /Users/cristianespinal/milagros-dashboard
npm run dev
```

Open http://localhost:3000 in the browser and check the browser console for errors. The map should work locally since .env.local has the token.

- [ ] **Step 2: If map works locally but not in production**

The issue is likely that the dynamic import of mapbox-gl fails in the Vercel production build. Try replacing the dynamic import approach with a direct import in the client component:

In `hero-map.tsx`, remove the async `init()` pattern and use a direct import:

```tsx
"use client";

import { useEffect, useRef, useState, useCallback } from "react";
import mapboxgl from "mapbox-gl";
```

Since `hero-map-loader.tsx` uses `next/dynamic` with `ssr: false`, mapbox-gl will only be imported on the client side, so the direct import should be safe.

- [ ] **Step 3: Add error boundary**

Wrap the map init in a try/catch that logs the error visibly:

```tsx
try {
  // map init code
} catch (err) {
  console.error("Map init failed:", err);
  setError(String(err));
}
```

Add an error state that shows the error on screen for debugging.

- [ ] **Step 4: Test, commit, deploy**

```bash
npm run build
git add -A && git commit -m "fix: resolve map rendering in production"
git push origin main
npx vercel --prod --yes
```

---

### Task 7: Refresh Dashboard Data and Redeploy

**Files:**
- Modify: `milagros-datalake/scripts/export_dashboard.py` (if needed)

- [ ] **Step 1: Re-export dashboard data with fresh Gold views**

```bash
cd /Users/cristianespinal/milagros-datalake
PYTHONPATH=. python scripts/export_dashboard.py --output /Users/cristianespinal/milagros-dashboard/public/data/
```

- [ ] **Step 2: Commit and deploy dashboard**

```bash
cd /Users/cristianespinal/milagros-dashboard
git add public/data/
git commit -m "data: refresh from data lake with Gold views"
git push origin main
npx vercel --prod --yes
```

- [ ] **Step 3: Push data lake fixes**

```bash
cd /Users/cristianespinal/milagros-datalake
git push origin main
```

---

### Task 8: Overnight Script (Unattended Execution)

**Files:**
- Create: `/tmp/milagros_overnight.sh`

This script runs Tasks 3-5 and 7 sequentially without human intervention.

- [ ] **Step 1: Create and run the overnight script**

```bash
cat > /tmp/milagros_overnight.sh << 'SCRIPT'
#!/bin/bash
set -e
export PYTHONPATH=.
cd /Users/cristianespinal/milagros-datalake

echo "=== $(date) OVERNIGHT PROCESSING START ==="

echo "--- Step 1: IDEAM DHIME ---"
python scripts/ingest_all.py --source ideam_dhime 2>&1 | tee -a /tmp/milagros_overnight_detail.log | tail -3
echo "IDEAM done at $(date)"

echo "--- Step 2: XM SiMEM ---"
python scripts/ingest_all.py --source xm_simem 2>&1 | tee -a /tmp/milagros_overnight_detail.log | tail -3
echo "XM done at $(date)"

echo "--- Step 3: HydroSHEDS ---"
python scripts/ingest_all.py --source hydrosheds 2>&1 | tee -a /tmp/milagros_overnight_detail.log | tail -3
echo "HydroSHEDS done at $(date)"

echo "--- Step 4: Process Silver ---"
python scripts/process_all.py --layer silver 2>&1 | tee -a /tmp/milagros_overnight_detail.log | tail -5
echo "Silver done at $(date)"

echo "--- Step 5: Process Gold ---"
python scripts/process_all.py --layer gold 2>&1 | tee -a /tmp/milagros_overnight_detail.log | tail -5
echo "Gold done at $(date)"

echo "--- Step 6: Validate ---"
python scripts/validate.py --layer all 2>&1 | tee -a /tmp/milagros_overnight_detail.log
echo "Validate done at $(date)"

echo "--- Step 7: Export Dashboard Data ---"
python scripts/export_dashboard.py --output /Users/cristianespinal/milagros-dashboard/public/data/ 2>&1 | tee -a /tmp/milagros_overnight_detail.log | tail -3
echo "Export done at $(date)"

echo "--- Step 8: Deploy Dashboard ---"
cd /Users/cristianespinal/milagros-dashboard
git add public/data/
git commit -m "data: overnight refresh from data lake" || true
git push origin main || true
npx vercel --prod --yes 2>&1 | tail -3
echo "Deploy done at $(date)"

echo "=== $(date) OVERNIGHT PROCESSING COMPLETE ==="
echo "Bronze:" && du -sh /Users/cristianespinal/milagros-datalake/bronze/
echo "Silver:" && du -sh /Users/cristianespinal/milagros-datalake/silver/
echo "Gold:" && du -sh /Users/cristianespinal/milagros-datalake/gold/
SCRIPT
chmod +x /tmp/milagros_overnight.sh
```

- [ ] **Step 2: Launch it**

```bash
nohup /tmp/milagros_overnight.sh >> /tmp/milagros_overnight.log 2>&1 &
echo "Monitor: tail -f /tmp/milagros_overnight.log"
```

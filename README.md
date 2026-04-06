# Central Hidroelectrica Milagros — Lago de Datos

> Repositorio centralizado de datos para los estudios de prefactibilidad y factibilidad de una central hidroelectrica de gran capacidad (>100 MW) en el norte de Antioquia, Colombia.

**Area de estudio:** San Pedro de los Milagros y municipios aledanos, norte de Antioquia
**Etapa:** Prefactibilidad avanzada
**Capacidad objetivo:** >100 MW (licenciamiento ANLA, despacho central obligatorio)
**Fuentes identificadas:** 80+ datasets en 13 categorias
**Ingestores registrados:** 49 | **Produciendo datos:** 47
**Almacenamiento:** 7.5 GB Bronze | 719 MB Silver | 16 MB Gold

---

## Inventario Completo de Fuentes de Datos

### 1. Hidrologia y Recursos Hidricos

| Fuente | Variables | Res. Temporal | Res. Espacial | Enlace | Costo |
|---|---|---|---|---|---|
| **IDEAM DHIME** | Caudal, nivel, T agua, sedimentos, precipitacion | Horaria-mensual | Estaciones puntuales | [dhime.ideam.gov.co](http://dhime.ideam.gov.co/atencionciudadano/) | Gratuito (registro) |
| **GloFAS v4** | Descarga fluvial, humedad suelo, escorrentia | Diaria | 0.05 (~5 km) | [CDS API](https://cds.climate.copernicus.eu/) | Gratuito (cuenta CDS) |
| **Open-Meteo Flood API** | Descarga fluvial, percentiles, 50 miembros ensemble | Diaria | 0.05 (~5 km) | [flood-api.open-meteo.com](https://flood-api.open-meteo.com) | Gratuito (10K req/dia) |
| **ERA5-Land** | Precip., ET, escorrentia, humedad suelo (4 capas), T, viento, radiacion — 50 variables | **Horaria** | 0.1 (~9 km) | [CDS API](https://cds.climate.copernicus.eu/) | Gratuito (cuenta CDS) |
| **HydroSHEDS/BASINS** | DEM condicionado, subcuencas Pfafstetter (niveles 1-12), red fluvial | Estatico | 3"-30" (90m-1km) | [hydrosheds.org](https://www.hydrosheds.org) | Gratuito |
| **GRDC** | Descarga diaria/mensual — 9,800+ estaciones globales | Diaria, mensual | Estaciones | [grdc.bafg.de](https://grdc.bafg.de) | Gratuito (no comercial) |

### 2. Meteorologia y Clima

| Fuente | Variables | Res. Temporal | Res. Espacial | Enlace | Costo |
|---|---|---|---|---|---|
| **IDEAM Estaciones Met.** | T, precip., HR, radiacion, viento, evaporacion, presion | Sub-horaria-anual | Estaciones | [dhime.ideam.gov.co](http://dhime.ideam.gov.co) | Gratuito (registro) |
| **CHIRPS v2** | Precipitacion (mm) | Diaria | **0.05 (~5.5 km)** | [data.chc.ucsb.edu](https://data.chc.ucsb.edu/products/CHIRPS-2.0/) | Gratuito |
| **PERSIANN-CCS-CDR** | Precipitacion (mm) | **3-horaria** | 0.04 (~4 km) | [chrsdata.eng.uci.edu](https://chrsdata.eng.uci.edu/) | Gratuito |
| **NASA POWER** | 300+ vars: T, precip., irradiancia, viento, HR, presion | Horaria (2001+), diaria (1981+) | 0.5x0.625 | [power.larc.nasa.gov](https://power.larc.nasa.gov/) | **Gratuito sin API key** |
| **Open-Meteo Weather** | T, precip., HR, viento, radiacion, humedad suelo | Horaria | ~9-25 km | [api.open-meteo.com](https://api.open-meteo.com) | Gratuito (10K req/dia) |
| **NOAA GSOD** | T media, rocio, presion, viento, precip. | Diaria | Estaciones | [ncei.noaa.gov](https://www.ncei.noaa.gov/data/global-summary-of-the-day/) | Gratuito |
| **CMIP6 (NEX-GDDP)** | Precip., T, HR, radiacion, viento — SSP1-2.6 a SSP5-8.5 | Diaria | 0.25 (~25 km) | [AWS S3](https://registry.opendata.aws/nex-gddp-cmip6/) | Gratuito |
| **CHELSA v2.1** | T, precip., 19 vars bioclimaticas + proyecciones CMIP6 | Mensual, climatologia | **30" (~1 km)** | [chelsa-climate.org](https://www.chelsa-climate.org/) | Gratuito |
| **WorldClim v2.1** | T, precip., radiacion, viento, presion vapor — 19 bioclimaticas | Mensual, climatologia | 30" (~1 km) | [worldclim.org](https://www.worldclim.org/) | Gratuito |

### 3. Mercado Electrico y Operacion

| Fuente | Variables | Enlace | Costo |
|---|---|---|---|
| **XM SiMEM** (213+ datasets) | Generacion real por planta, precio bolsa, demanda, aportes hidricos, embalses | [simem.xm.com.co](https://simem.xm.com.co) | Gratuito |
| **XM PARATEC** | Parametros tecnicos de generadores, lineas, subestaciones | [paratec.xm.com.co](https://paratec.xm.com.co) | Gratuito |
| **UPME Registro Proyectos** | Proyectos generacion fases 1-3, planes expansion, certificacion FNCER | [upme.gov.co](https://www.upme.gov.co/inscripcion-de-proyectos-de-generacion/) | Gratuito |
| **CREG (Alejandria)** | Resoluciones regulatorias PCH, formulas tarifarias, cargo confiabilidad | [gestornormativo.creg.gov.co](https://gestornormativo.creg.gov.co/) | Gratuito |
| **SUI/SSPD** | Capacidad instalada, generacion, tarifas, cobertura, calidad | [sui.superservicios.gov.co](https://sui.superservicios.gov.co/) | Gratuito |
| **EPM** | Portafolio plantas, reportes sostenibilidad, transferencias sector electrico | [epm.com.co](https://www.epm.com.co/institucional/sobre-epm/nuestras-plantas) | Gratuito |

### 4. Geoespacial y Topografia

| Fuente | Resolucion | Enlace | Costo |
|---|---|---|---|
| **IGAC** (cartografia oficial) | LIDAR 1m, TanDEM-X 12m, SRTM 30m; cartografia 1:25K a 1:500K | [geoportal.igac.gov.co](https://geoportal.igac.gov.co) | Gratuito (CC BY-SA 4.0) |
| **NASA SRTM** | **30 m** (DSM, Feb 2000) | [earthexplorer.usgs.gov](https://earthexplorer.usgs.gov) | Gratuito (registro) |
| **ALOS PALSAR DEM** | **12.5 m** (RTC, banda L penetra vegetacion) | [search.asf.alaska.edu](https://search.asf.alaska.edu) | Gratuito (Earthdata) |
| **Copernicus DEM GLO-30** | **30 m** (DSM, 2011-2015, mas moderno) | [AWS S3 (sin auth)](https://registry.opendata.aws/copernicus-dem/) | **Gratuito sin auth** |
| **TanDEM-X** | **~12 m** (DSM, mejor resolucion gratuita global) | [tandemx-science.dlr.de](https://tandemx-science.dlr.de) | Gratuito (4-8 semanas) |
| **OpenStreetMap** | Vector (vias, edificaciones, lineas electricas) | [overpass-turbo.eu](https://overpass-turbo.eu/) | Gratuito (ODbL) |
| **Google Earth Engine** | Multi-petabyte: todos los datasets satelitales | [earthengine.google.com](https://earthengine.google.com) | Gratuito (investigacion) |
| **DANE Geoportal** | Municipal, sector censal, vereda | [geoportal.dane.gov.co](https://geoportal.dane.gov.co) | Gratuito |

### 5. Teledeteccion y Sensores Satelitales

| Fuente | Resolucion | Revisita | Enlace | Costo |
|---|---|---|---|---|
| **Sentinel-1 SAR** | 5m x 20m (IW) | **6 dias** | [Copernicus Dataspace](https://dataspace.copernicus.eu/) | Gratuito |
| **Sentinel-2 MSI** | **10m** (visible), 20m (red-edge) | ~5 dias | [Copernicus Dataspace](https://dataspace.copernicus.eu/) | Gratuito |
| **Landsat 8/9** | **30m** (MS), 15m (PAN), 100m (termico) | 8 dias | [earthexplorer.usgs.gov](https://earthexplorer.usgs.gov) | Gratuito |
| **MODIS** | 250m-1km | Diaria | [LAADS](https://ladsweb.modaps.eosdis.nasa.gov/) | Gratuito |
| **Planet Labs** | **3m** (diario), 50cm (tasking) | **Diaria** | [planet.com](https://api.planet.com/) | **Pago** (gratis NICFI/edu) |
| **Maxar** | **31cm** (pan WV-3), 8 bandas MS | Tasking | [maxar.com](https://www.maxar.com) | **Pago (~$15-25/km2)** |
| **VIIRS** | 375m-750m (incendios, luces nocturnas) | Diaria | [FIRMS](https://firms.modaps.eosdis.nasa.gov) | Gratuito |
| **ICESat-2** | 17m footprint (altimetria agua) | 91 dias | [nsidc.org](https://nsidc.org/data/icesat-2) | Gratuito |
| **GEDI** | 25m footprint (altura dosel, biomasa) | 2019-2023 | [gedi.umd.edu](https://gedi.umd.edu) | Gratuito |
| **MS Planetary Computer** | Multi-petabyte (S1/S2, Landsat, DEM, ERA5) | Variable | [planetarycomputer.microsoft.com](https://planetarycomputer.microsoft.com) | Gratuito |

### 6. Calidad del Agua y Monitoreo Ambiental

| Fuente | Variables | Enlace | Costo |
|---|---|---|---|
| **CORANTIOQUIA / PIRAGUA** | pH, OD, DBO, DQO, turbidez, conductividad, caudal ecologico | [corantioquia.gov.co](https://www.corantioquia.gov.co/recurso-agua/) | Gratuito (solicitud formal para datos crudos) |
| **IDEAM Red de Calidad** | ICA nacional, DBO, DQO, OD, pH, turbidez, SST, nutrientes | [sirh.ideam.gov.co](http://sirh.ideam.gov.co) | Gratuito |
| **ANLA VITAL** | EIA, PMA, ICA (informes cumplimiento ambiental) | [vital.anla.gov.co](https://vital.anla.gov.co) | Gratuito (registro) |
| **GEMStat** | 659+ parametros fisicoquimicos, 31M+ registros | [gemstat.org](https://gemstat.org) | Gratuito (no comercial) |
| **Copernicus Water Quality** | Estado trofico, clorofila-a, turbidez, T superficial | [land.copernicus.eu](https://land.copernicus.eu/global/products/lwq) | Gratuito |

### 7. Ecosistemas y Biodiversidad

| Fuente | Variables | Res. Espacial | Enlace | Costo |
|---|---|---|---|---|
| **IDEAM Corine Land Cover** | Cobertura/uso suelo — 5 epocas (2000-2018) | 1:100,000 | [SIAC](https://siac-datosabiertos-mads.hub.arcgis.com) | Gratuito |
| **MapBiomas Colombia** | Cobertura anual 25 clases, deforestacion — **1985-2024** | **30 m anual** | [colombia.mapbiomas.org](https://colombia.mapbiomas.org) | Gratuito (CC BY-SA 4.0) |
| **GBIF / SiB Colombia** | 1,600M+ registros de especies georreferenciados | Puntuales | [gbif.org](https://api.gbif.org/v1/) | Gratuito |
| **Instituto Humboldt** | 927K+ registros biologicos, modelos distribucion, ecosistemas | 1:100K | [humboldt.org.co](http://geonetwork.humboldt.org.co) | Gratuito |
| **RUNAP** | Areas protegidas SINAP: PNN, DMI, DRMI, RNSC | Vector | [runap.parquesnacionales.gov.co](https://runap.parquesnacionales.gov.co) | Gratuito |
| **FAO GeoNetwork** | Suelos (HWSD), zonas agroecologicas (GAEZ) | 1 km | [data.apps.fao.org](https://data.apps.fao.org) | Gratuito |

### 8. Geologia, Geotecnia y Amenazas Naturales

| Fuente | Variables | Enlace | Costo |
|---|---|---|---|
| **SGC Mapa Geologico** | Unidades litologicas, formaciones, fallas, cronoestratigrafia | [srvags.sgc.gov.co](http://srvags.sgc.gov.co/Flexviewer/Mapa_Geologico_Colombia_2015/) | Gratuito |
| **SGC Amenaza Sismica** | Aa, Av, Ae, Ad; espectros; PGA — NSR-10 | [amenazasismica.sgc.gov.co](https://amenazasismica.sgc.gov.co/) | Gratuito |
| **SGC RSNC Catalogo Sismico** | Origen, ubicacion, profundidad, magnitud, mecanismo focal | [sgc.gov.co](https://www.sgc.gov.co) | Gratuito |
| **SGC SIMMA** | Tipo deslizamiento, disparador, area, litologia — **32,000+ registros** | [simma.sgc.gov.co](https://simma.sgc.gov.co/) | Gratuito |
| **USGS ComCat** | Catalogo sismico global, ShakeMap, PAGER | [earthquake.usgs.gov](https://earthquake.usgs.gov/fdsnws/event/1/) | **Gratuito sin auth** |
| **NASA LHASA v2** | Nowcast peligro deslizamiento (cat. 1-5) | [landslides.nasa.gov](https://landslides.nasa.gov/viewer) | Gratuito (Earthdata) |
| **UNGRD** | Registros historicos desastres, alertas tempranas | [portal.gestiondelriesgo.gov.co](https://portal.gestiondelriesgo.gov.co) | Gratuito |
| **DesInventar Antioquia** | Desastres 1903-2023: tipo, fecha, muertes, heridos, viviendas | [db.desinventar.org](https://db.desinventar.org) | Gratuito (Apache 2.0) |

### 9. Potencial Solar y Eolico Complementario

| Fuente | Variables | Res. Espacial | Enlace | Costo |
|---|---|---|---|---|
| **Global Solar Atlas v2.6** | GHI, DNI, DIF, GTI, **PVOUT (kWh/kWp)** | **250 m** | [globalsolaratlas.info](https://globalsolaratlas.info) | Gratuito (CC BY 4.0) |
| **Global Wind Atlas v3** | Velocidad viento, densidad potencia, Weibull (10-200m) | **~250 m** | [globalwindatlas.info](https://globalwindatlas.info) | Gratuito (CC BY 4.0) |
| **IDEAM Atlas Solar** | GHI, DNI, DIF, brillo solar, UV | Estaciones | [repositoriobi.minenergia.gov.co](https://repositoriobi.minenergia.gov.co/handle/123456789/2414) | Gratuito |
| **UPME Atlas Viento** | Velocidad media 10/20/50m, direccion, densidad potencia | Estaciones | [upme.gov.co](https://www1.upme.gov.co/) | Gratuito |
| **NREL NSRDB** | GHI, DNI, DHI, T, rocio, HR, viento, presion | **4 km; semi-horaria** | [nsrdb.nrel.gov](https://nsrdb.nrel.gov/) | Gratuito (registro) |

### 10. Datos Socioeconomicos y Territoriales

| Fuente | Variables | Enlace | Costo |
|---|---|---|---|
| **DANE CNPV 2018** | Poblacion, demografia, vivienda, servicios, educacion | [sitios.dane.gov.co/cnpv](https://sitios.dane.gov.co/cnpv/) | Gratuito |
| **DANE Proyecciones** | Poblacion por sexo, quinquenios, urbano/rural — 2018-2050 | [dane.gov.co](https://www.dane.gov.co) | Gratuito |
| **DNP TerriData** | **800+ indicadores en 16 dimensiones**: pobreza, Gini, educacion, salud, ODS | [terridata.dnp.gov.co](https://terridata.dnp.gov.co/) | Gratuito |
| **AGRONET/EVA** | Area sembrada/cosechada, produccion, rendimiento, pecuario | [datos.gov.co](https://www.datos.gov.co/resource/uejq-wxrr.json) | Gratuito |
| **Gobernacion Antioquia** | Poblacion, salud, educacion, produccion — 125 municipios | [antioquiadatos.gov.co](https://www.antioquiadatos.gov.co/) | Gratuito |
| **EOT/PBOT San Pedro** | Clasificacion uso suelo, diagnostico, vias, saneamiento | [colombiaot.gov.co](https://www.colombiaot.gov.co/pot/) | Consulta en Alcaldia |
| **Catastro Antioquia** | Identificacion predial, area, uso, avaluo catastral | [catastroantioquia.gov.co](https://www.catastroantioquia.gov.co/) | Gratuito (datos geo); solicitud (detalle) |

### 11. Infraestructura y Conectividad

| Fuente | Variables | Enlace | Costo |
|---|---|---|---|
| **MinTIC Geoportal** | Cobertura movil 2G/3G/4G/5G, internet fijo, fibra optica | [ontic.mintic.gov.co](https://ontic.mintic.gov.co/portal/Secciones/Geoportal/) | Gratuito |
| **INVIAS SIV** | Red vial primaria/secundaria/terciaria, estado, trafico | [hermes2.invias.gov.co](https://hermes2.invias.gov.co/SIV/) | Gratuito |
| **UPME Geoportal** | Lineas STN (500/220/110 kV), subestaciones, plantas | [upme.gov.co](https://www1.upme.gov.co/siame/Paginas/Geoportal.aspx) | Gratuito |

### 12. Marco Regulatorio

| Marco | Contenido | Enlace |
|---|---|---|
| **Decreto 1076/2015** | Decreto Unico ambiental: licenciamiento, concesiones agua, POMCA, EIA | [minambiente.gov.co](https://www.minambiente.gov.co) |
| **Ley 1715/2014** | PCH = FNCER. Incentivos: deduccion 50% renta, exclusion IVA, exencion arancel | [secretariasenado.gov.co](http://www.secretariasenado.gov.co/senado/basedoc/ley_1715_2014.html) |
| **Ley 2099/2021** | Transicion energetica. Extiende incentivos 30 anos | [funcionpublica.gov.co](https://www.funcionpublica.gov.co/eva/gestornormativo/norma.php?i=166326) |
| **Ley 99/1993 Art.45** | Transferencias 6% ventas energia a municipios y CARs del area de embalse | — |
| **CREG Res. 086/1996** | Reglas PCH <20 MW | [gestornormativo.creg.gov.co](https://gestornormativo.creg.gov.co/) |
| **CORANTIOQUIA** | Concesion aguas, licencias <100 MW, POMCA Rio Grande | [corantioquia.gov.co](https://www.corantioquia.gov.co) |
| **ANLA VITAL** | Expedientes ambientales, EIA, resoluciones | [vital.anla.gov.co](https://vital.anla.gov.co) |

> **Nota sobre competencia regulatoria:** ANLA licencia plantas >=100 MW. CORANTIOQUIA licencia plantas <100 MW. Para este proyecto (>100 MW), ANLA es la autoridad competente.

### Resumen de Costos

| Categoria | Entradas catalogo | Gratuitas | Requieren registro gratuito | Requieren solicitud formal | De pago |
|---|---|---|---|---|---|
| Hidrologia | 427 | 5 | 1 (IDEAM) | — | — |
| Meteorologia | 514 | 8 | 1 (CDS) | — | — |
| Mercado electrico | 32 | 6 | — | — | — |
| Geoespacial | 13 | 7 | 2 (USGS, GEE) | 1 (TanDEM-X) | — |
| Teledeteccion | 166 | 7 | 2 (Earthdata) | — | 2 (Planet, Maxar) |
| Calidad agua | 26 | 4 | 1 (ANLA) | 1 (CORANTIOQUIA datos crudos) | — |
| Biodiversidad | 36 | 6 | — | — | — |
| Geologia | 84 | 8 | — | — | — |
| Solar/eolico | 14 | 4 | 1 (NREL) | — | — |
| Socioeconomico | 45 | 6 | 1 (microdatos DANE) | 1 (EOT/PBOT) | — |
| Infraestructura | 14 | 3 | — | — | — |
| Regulatorio | 15 | — | — | 1 (CREG/ANLA) | — |
| **Total** | **1,386** | **~65** | **~10** | **~3** | **2** |

**~95% de las fuentes son gratuitas y abiertas.** Solo Planet Labs y Maxar requieren pago (y pueden no ser necesarias si Sentinel-2 a 10m es suficiente).

---

## Roadmap y Productos

### Fase 1: Infraestructura + Datos Criticos (Semanas 1-2) -- COMPLETADA

**22 ingestores operativos** cubriendo las fuentes mas urgentes para la prefactibilidad:

| Categoria | Ingestores | Fuentes |
|---|---|---|
| Hidrologia | 3 | IDEAM DHIME, GloFAS, HydroSHEDS |
| Meteorologia | 4 | ERA5-Land, CHIRPS, NASA POWER, Open-Meteo |
| Geologia/Amenazas | 5 | SGC Geologia, Sismicidad, SIMMA, Amenaza sismica, DesInventar |
| Geoespacial | 3 | DEMs (Copernicus+SRTM+ALOS), IGAC cartografia |
| Mercado electrico | 2 | XM SiMEM, UPME Proyectos |
| Socioeconomico | 3 | DANE Censo, DNP TerriData, AGRONET |
| Biodiversidad | 3 | MapBiomas, Corine Land Cover, RUNAP |
| Regulatorio | 1 | CORANTIOQUIA POMCA |
| **Total Fase 1** | **24** | |

**Productos de Fase 1:**

| Producto (Gold View) | Descripcion | Uso directo |
|---|---|---|
| **Balance Hidrico** | P - ET - Q por subcuenca, mensual, 1950-presente | Dimensionar disponibilidad de agua |
| **Series de Caudal** | Serie continua diaria ~70 anos con indicador de confianza | Caudales de diseno, analisis de frecuencia |
| **Curvas de Duracion** | Percentiles 5-95 de caudal, anual y mensual | Caudal firme (Q95), caudal medio |
| **Potencial de Generacion** | P = Q x H x eta para combinaciones caudal-salto | Estimar MW instalables por alternativa |
| **Perfil Geologico** | Litologia, fallas, deslizamientos, pendientes | Restricciones de localizacion de presa |
| **Amenazas Naturales** | Mapa multicriterio: sismica, deslizamiento, inundacion | Evaluacion de riesgo, input al EIA |
| **Linea Base Ambiental** | Cobertura, areas protegidas, ecosistemas, biodiversidad | Capitulo ambiental del EIA |
| **Mercado y Despacho** | Precio bolsa, factor planta, proyectos competidores | Viabilidad financiera |
| **Indicadores Socioeconomicos** | Perfil municipal: poblacion, NBI, Gini, produccion | Capitulo socioeconomico del EIA |
| **Recurso Solar/Eolico** | GHI, DNI, velocidad viento por punto del AOI | Evaluacion de solar flotante/complementariedad |

### Fase 2: Datos Complementarios (Semanas 3-4) -- COMPLETADA

18 ingestores adicionales para teledeteccion, clima, biodiversidad, infraestructura:

- Sentinel-1 SAR (InSAR para deformacion de presa, inundaciones)
- Sentinel-2 (indices espectrales: NDVI, NDWI, turbidez)
- Landsat 8/9 (series historicas continuas)
- PERSIANN (precipitacion sub-diaria)
- CHELSA + WorldClim (climatologia 1km, downscaling orografico)
- CMIP6 (escenarios cambio climatico SSP1-SSP5)
- Global Solar Atlas + Wind Atlas (250m)
- NREL NSRDB (radiacion semi-horaria)
- GBIF/SiB + Humboldt (biodiversidad detallada)
- FAO suelos (HWSD, GAEZ)
- INVIAS + MinTIC + UPME red electrica

### Fase 3: Datos Especializados (Semanas 5-6) -- COMPLETADA

15+ ingestores para monitoreo avanzado:

- Copernicus Water Quality (estado trofico embalse)
- ICESat-2 (altimetria satelital de nivel de agua)
- GEDI (estructura vegetacion, biomasa)
- MODIS/VIIRS (incendios, luces nocturnas, LST)
- NASA LHASA v2 (nowcast deslizamientos 1km)
- Catastro Antioquia + OSM (predios, infraestructura local)
- Planet Labs (si disponible via NICFI/educacion)

### Fase 4: Documentos y Regulatorio (Continuo) -- COMPLETADA

Pipeline de ingestion documental:

- ANLA VITAL (expedientes EIA hidroelectricas)
- CREG Alejandria (resoluciones tarifarias)
- CORANTIOQUIA (POMCA, concesiones, monitoreo)
- UPME (registros, certificaciones)
- EOT/PBOT San Pedro (ordenamiento territorial)

### Productos Finales del Roadmap

Al completar las 4 fases, el lago de datos permite:

1. **Modelo hidrologico calibrado** con 70+ anos de datos y multiples fuentes cruzadas
2. **Mapa de aptitud** para localizacion de presa (geologia + amenazas + restricciones)
3. **Estimacion de potencial** con curvas de duracion y barrido de alternativas Q x H
4. **Linea base ambiental completa** para el EIA ante ANLA
5. **Analisis de mercado** con series historicas de precios y proyectos competidores
6. **Paquetes regulatorios** listos para radicacion ante ANLA, CORANTIOQUIA, UPME
7. **Evaluacion de complementariedad** solar flotante + eolica sobre/cerca del embalse
8. **Monitoreo de cambio climatico** con proyecciones CMIP6 a 2100

---

## Exports por Audiencia

| Audiencia | Comando | Formatos | Contenido |
|---|---|---|---|
| Consultores hidrologos | `export_consultor.py --disciplina hidrologia` | GeoPackage, Excel, NetCDF | Balance hidrico, caudales, cuencas, DEM |
| Consultores geologos | `export_consultor.py --disciplina geologia` | GeoPackage, Excel | Geologia, fallas, amenazas, pendientes |
| Consultores ambientales | `export_consultor.py --disciplina ambiental` | GeoPackage, Excel, CSV | Cobertura, biodiversidad, areas protegidas |
| Ingenieros electricos | `export_consultor.py --disciplina electrico` | Excel, CSV | Mercado, potencial generacion, recurso solar |
| Inversionistas | `export_inversionistas.py` | PDF ejecutivo, Excel resumen | Indicadores clave, capacidad, mercado |
| ANLA | `export_regulador.py --entidad anla` | GDB, Shapefile, Excel, PDF | Paquete EIA completo |
| CORANTIOQUIA | `export_regulador.py --entidad corantioquia` | GDB, Shapefile, Excel | Concesion de aguas |
| UPME | `export_regulador.py --entidad upme` | Excel | Registro de proyecto |

---

## Area de Estudio

**Bounding box:** `W: -75.80, S: 6.25, E: -75.25, N: 6.70` (~55 km x ~50 km)

**Municipios incluidos (codigo DANE):**

| Codigo | Municipio |
|---|---|
| 05664 | San Pedro de los Milagros |
| 05264 | Entrerrios |
| 05086 | Belmira |
| 05237 | Donmatias |
| 05686 | Santa Rosa de Osos |
| 05079 | Barbosa |
| 05088 | Bello |
| 05761 | Sopetran |
| 05576 | Olaya |
| 05042 | Santafe de Antioquia |

**Zona de amenaza sismica:** Intermedia (NSR-10: Aa ~ 0.15, Av ~ 0.20)

**Autoridad ambiental competente:** ANLA (>100 MW) + CORANTIOQUIA (Oficina Tahamies)

---

## Documentacion Tecnica

### Arquitectura: Lake Medallion con Motor Espacial

```
Bronze (crudo, inmutable)  -->  Silver (limpio, estandarizado)  -->  Gold (analitico, cross-dominio)
       |                              |                                    |
  CSV, JSON, GeoTIFF,          Parquet, COG GeoTIFF,              Parquet, GeoParquet
  NetCDF, Shapefile,           GeoParquet (EPSG:4326)             (vistas derivadas)
  GDB, PDF                          |
       |                      DuckDB + spatial ext.
  Catalogo DuckDB                    |
  (linaje completo)           Procesadores Python
```

- **Bronze:** Dato crudo tal cual llega. Inmutable. Es el registro de verdad.
- **Silver:** Limpio, estandarizado, recortado al AOI. Reproducible desde Bronze.
- **Gold:** Vistas analiticas cross-dominio. Reproducible desde Silver.
- **Catalogo:** DuckDB registra cada archivo con fuente, fecha, hash, variables, cobertura.

### Instalacion

```bash
git clone https://github.com/Cespial/milagros-datalake.git
cd milagros-datalake
cp .env.example .env   # Llenar API keys
pip install -e ".[dev]"
python -c "from config.settings import ensure_dirs; ensure_dirs()"
```

**Registros gratuitos necesarios:**

| Servicio | URL | Tiempo |
|---|---|---|
| Copernicus CDS | https://cds.climate.copernicus.eu/user/register | 5 min |
| NASA Earthdata | https://urs.earthdata.nasa.gov/users/new | 5 min |
| Google Earth Engine | https://earthengine.google.com/signup | 1-3 dias |
| NREL Developer | https://developer.nrel.gov/signup | 5 min |
| GBIF | https://www.gbif.org/user/profile | 5 min |

### Uso

```bash
# Ingestar fuentes que no requieren auth
PYTHONPATH=. python scripts/ingest_all.py --source nasa_power
PYTHONPATH=. python scripts/ingest_all.py --source sgc_sismicidad
PYTHONPATH=. python scripts/ingest_all.py --source dane_censo

# Ingestar toda la Fase 1 (requiere API keys configuradas)
PYTHONPATH=. python scripts/ingest_all.py --phase 1

# Procesar Bronze -> Silver -> Gold
PYTHONPATH=. python scripts/process_all.py --layer silver
PYTHONPATH=. python scripts/process_all.py --layer gold

# Validar integridad
PYTHONPATH=. python scripts/validate.py --layer all

# Exportar paquetes
PYTHONPATH=. python scripts/export_consultor.py --disciplina hidrologia
PYTHONPATH=. python scripts/export_regulador.py --entidad anla

# Dry-run (ver que se ejecutaria sin ejecutar)
PYTHONPATH=. python scripts/ingest_all.py --phase 1 --dry-run
PYTHONPATH=. python scripts/process_all.py --layer gold --dry-run
```

### Stack Tecnologico

| Componente | Tecnologia |
|---|---|
| Motor de consulta | DuckDB + extension spatial |
| Tabular | Parquet (PyArrow, pandas) |
| Raster | COG GeoTIFF (rasterio, rioxarray, xarray) |
| Vector | GeoParquet (geopandas, shapely, fiona) |
| Catalogo | DuckDB (`catalog/catalog.duckdb`) |
| APIs | httpx, cdsapi, earthengine-api, pydataxm |
| CLI | Click |
| Logging | structlog |
| Retry | tenacity (backoff exponencial) |
| Hashing | xxhash |
| Tests | pytest |

### Estructura de Directorios

```
milagros-datalake/
|-- config/
|   |-- settings.py           # AOI, CRS, rutas
|   |-- sources.yaml          # Catalogo 22+ fuentes
|   +-- .env                  # API keys (no versionado)
|-- catalog/
|   |-- catalog.duckdb        # Metadatos de todo el lake
|   +-- stac/                 # JSON sidecar raster
|-- bronze/                   # Crudo, inmutable
|   |-- tabular/              # CSV, JSON, Excel
|   |-- raster/               # GeoTIFF, NetCDF, GRIB
|   |-- vector/               # Shapefile, GeoJSON, GDB
|   +-- documents/            # PDFs regulatorios
|-- silver/                   # Limpio, estandarizado
|   |-- tabular/              # Parquet particionado (year=YYYY/)
|   |-- raster/               # COG GeoTIFF, CRS unificado
|   +-- vector/               # GeoParquet, EPSG:4326
|-- gold/                     # Vistas analiticas
|   |-- balance_hidrico.parquet
|   |-- series_caudal.parquet
|   |-- curvas_duracion.parquet
|   |-- potencial_generacion.parquet
|   |-- perfil_geologico.geoparquet
|   |-- amenazas_naturales.parquet
|   |-- mercado_despacho.parquet
|   |-- indicadores_socioeconomicos.parquet
|   |-- linea_base_ambiental.geoparquet
|   +-- recurso_solar_eolico.parquet
|-- exports/                  # Paquetes por audiencia
|   |-- consultores/
|   |-- inversionistas/
|   +-- reguladores/
|-- ingestors/                # 49 modulos registrados (47 produciendo datos)
|-- processors/               # Bronze->Silver->Gold
|   |-- tabular/
|   |-- raster/
|   |-- vector/
|   +-- gold/                 # 10 vistas analiticas
|-- scripts/                  # CLIs
|-- tests/                    # 22 tests
+-- docs/                     # Specs y planes
```

### Convenciones de Datos Silver

| Aspecto | Convencion |
|---|---|
| Columnas | `snake_case` espanol: `caudal_m3s`, `precipitacion_mm` |
| Timestamps | UTC, `TIMESTAMP WITH TIME ZONE` |
| Coordenadas | `lat`/`lon` DOUBLE, WGS84 |
| Unidades | SI: m3/s, mm, C, m, kW, kWh, ha, km2 |
| Nulos | `NULL` nativo (no strings "-999", "N/A") |
| Codigos municipio | DANE 5 digitos: `05664` |
| CRS | EPSG:4326 (Silver); EPSG:3116 solo para calculos de area |
| Particionado | `{categoria}/year={YYYY}/data.parquet` |

### Almacenamiento Estimado

| Capa | Estimacion |
|---|---|
| Bronze tabular | ~5 GB |
| Bronze raster | ~30 GB |
| Bronze vector | ~2 GB |
| Bronze documents | ~1 GB |
| Silver tabular | ~3 GB |
| Silver raster | ~15 GB |
| Silver vector | ~1 GB |
| Gold | ~2 GB |
| **Total** | **~60 GB** |

### Reproducibilidad

Los datos NO se versionan en git. La reproducibilidad se garantiza por codigo:

```bash
# Cualquiera con el repo + .env puede reconstruir el lake completo
git clone https://github.com/Cespial/milagros-datalake.git
cd milagros-datalake
cp .env.example .env  # + llenar keys
pip install -e .
PYTHONPATH=. python scripts/ingest_all.py --phase 1
PYTHONPATH=. python scripts/process_all.py --layer silver
PYTHONPATH=. python scripts/process_all.py --layer gold
```

---

## Contexto del Sistema Hidroelectrico Existente

La infraestructura hidroelectrica actual en la zona de San Pedro de los Milagros corresponde al **sistema Aprovechamiento Multiple del Rio Grande** (EPM):

| Componente | Especificaciones |
|---|---|
| **Embalse Riogrande II** | 245 Mm3, 1,100 ha espejo, cota ~2,270 msnm, presa 65m (relleno de tierra) |
| **Central La Tasajera** | 306 MW (3x102 MW Pelton), salto 933m, caverna subterranea |
| **Central Niquia** | ~20 MW (1 Francis), salto 420.5m |
| **Central Riogrande I (Mocorongo)** | ~19-25 MW, entrada servicio 1951 |
| **Funcion dual** | Generacion electrica + agua potable Valle de Aburra |
| **Conexion SIN** | 230 kV (subestacion encapsulada SF6) |

El proyecto Milagros es **independiente** de este sistema existente.

---

## Licencia

Codigo: MIT. Datos: cada fuente tiene su propia licencia (ver tabla de inventario).

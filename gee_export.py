"""
gee_export.py — Multi-region GEE export pipeline.

Core optimisation: collapse each chunk's ImageCollection to a SINGLE composite
image server-side (mean or mode), then call reduceRegions ONCE per chunk over
ALL cells. This means exactly 1 getInfo() per chunk regardless of how many
images or cells are in it — O(chunks) instead of O(images × cell_batches).

Per-date time series are preserved by iterating over date-windows within the
chunk (one composite per day/8-day period depending on the variable).

Usage:
  python gee_export.py                        # all regions, all vars
  python gee_export.py --region italia
  python gee_export.py --var ndvi --var lst
  python gee_export.py --dry-run
"""

import argparse, json, logging, math, sys, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import ee
import pandas as pd
import yaml
from tqdm import tqdm

# ── CLI ────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--config",  default="config.yaml")
parser.add_argument("--region",  action="append", dest="regions")
parser.add_argument("--var",     action="append", dest="vars")
parser.add_argument("--dry-run", action="store_true")
args = parser.parse_args()

# ── Config ─────────────────────────────────────────────────────────────────
with open(args.config) as f:
    cfg = yaml.safe_load(f)

secrets_path = Path(args.config).parent / "secrets.yaml"
if secrets_path.exists():
    with open(secrets_path) as f:
        sec = yaml.safe_load(f) or {}
    for k, v in sec.items():
        if isinstance(v, dict) and isinstance(cfg.get(k), dict):
            cfg[k].update(v)
        else:
            cfg[k] = v

log_dir = Path(cfg["LOGGING"]["log_file"]).parent
log_dir.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=getattr(logging, cfg["LOGGING"]["level"]),
    format="%(asctime)s %(levelname)-8s %(message)s",
    handlers=[logging.FileHandler(cfg["LOGGING"]["log_file"]),
              logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("gee_export")

# ── GEE init ───────────────────────────────────────────────────────────────
gee_project = cfg.get("GEE", {}).get("project")
if not gee_project or gee_project == "your-gee-project-id":
    log.error("Set GEE.project in secrets.yaml"); sys.exit(1)

log.info(f"Initialising Earth Engine (project: {gee_project})...")
try:
    ee.Initialize(project=gee_project)
except Exception:
    ee.Authenticate(); ee.Initialize(project=gee_project)
log.info("Earth Engine OK")

# ── Per-variable temporal resolution ──────────────────────────────────────
# "daily"  → one composite per calendar day   (LST, CHIRPS, S5P, FIRMS …)
# "8day"   → one composite per 8-day window   (LAI, ET, albedo, MODIS LST)
# "16day"  → one composite per 16-day window  (EVI)
# "scene"  → one composite per S2 overpass    (NDVI/NDWI/NDBI — keep all)
# "period" → single composite for whole chunk (LULC modal)
# "3day"   → one composite per 3-day window   (SMAP)
# Resolution is chosen per-variable AND per-region at runtime.
# For small regions (few cells): use fine temporal resolution (daily/8day).
# For large regions (many cells): use "period" = one composite per chunk.
# Threshold: if n_cells * windows_per_chunk > 4000, collapse to "period".
# These are the *default* resolutions (used for small regions):
VAR_RESOLUTION = {
    "ndvi":               "scene",
    "ndwi":               "scene",
    "ndbi":               "scene",
    "evi":                "16day",
    "lst":                "daily",
    "precipitation":      "daily",
    "soil_moisture":      "3day",
    "air_temp":           "daily",
    "humidity":           "daily",
    "wind_speed":         "daily",
    "no2":                "daily",
    "co":                 "daily",
    "fires":              "daily",
    "lulc":               "period",
    "albedo":             "8day",
    "lai":                "8day",
    "snow_cover":         "daily",
    "evapotranspiration": "8day",
    "aerosol":            "daily",
    "o3":                 "daily",
}

WINDOWS_PER_MONTH = {
    "daily": 30, "3day": 10, "8day": 4, "16day": 2, "period": 1, "scene": 25,
}
MAX_CALLS_PER_CHUNK = 4000  # n_cells * windows -> if over, collapse to period

def effective_resolution(var_id: str, n_cells: int, chunk_months: int) -> str:
    """
    Each sub-window is ONE reduceRegions call returning n_cells features.
    The limit is per-call, not per-chunk — so we only need n_cells <= 5000.
    All regions with <= 4500 cells can use fine-grained resolution.
    Only collapse to "period" if n_cells itself exceeds the limit (unlikely).
    """
    base = VAR_RESOLUTION.get(var_id, "daily")
    if base == "period":
        return "period"
    if n_cells > MAX_CALLS_PER_CHUNK:
        return "period"
    return base

CHUNK_MONTHS = {
    "ndvi":1,"ndwi":1,"ndbi":1,"evi":3,"lst":1,"precipitation":1,
    "soil_moisture":3,"air_temp":1,"humidity":1,"wind_speed":1,
    "no2":1,"co":1,"fires":1,"lulc":1,"albedo":3,"lai":3,
    "snow_cover":1,"evapotranspiration":3,"aerosol":1,"o3":1,
}
DEFAULT_CHUNK_MONTHS = cfg["CHUNKING"]["chunk_months"]

# Native resolution of each dataset (metres).
# reduceRegions is fastest at the dataset's native scale.
# The region's scale_m is used as a MINIMUM — we always use
# max(region_scale, native_scale) so we never upsample.
VAR_NATIVE_SCALE = {
    "ndvi": 10, "ndwi": 10, "ndbi": 10,   # Sentinel-2 10m
    "evi": 250,                             # MODIS MOD13Q1
    "lst": 1000,                            # MODIS MOD11A1
    "precipitation": 5566,                  # CHIRPS ~5.5km
    "soil_moisture": 11000,                 # SMAP ~11km
    "air_temp": 11132, "humidity": 11132, "wind_speed": 11132,  # ERA5 ~11km
    "no2": 3500, "co": 3500, "fires": 1000,
    "lulc": 10,                             # Dynamic World
    "albedo": 500,                          # MODIS MCD43A3
    "lai": 500,                             # MODIS MOD15A2H
    "snow_cover": 500,
    "evapotranspiration": 500,
    "aerosol": 3500, "o3": 3500,
}

def effective_scale(var_id: str, region_scale: int) -> int:
    native = VAR_NATIVE_SCALE.get(var_id, region_scale)
    return max(region_scale, native)

# ── Grid ───────────────────────────────────────────────────────────────────
def build_grid(aoi_geojson, cell_km):
    coords = aoi_geojson["features"][0]["geometry"]["coordinates"][0]
    lons = [c[0] for c in coords]; lats = [c[1] for c in coords]
    lon_min, lon_max = min(lons), max(lons)
    lat_min, lat_max = min(lats), max(lats)
    d_lat = cell_km / 111.0
    d_lon = cell_km / (111.0 * math.cos(math.radians((lat_min + lat_max) / 2)))
    cells, cid = [], 0
    lat = lat_min
    while lat < lat_max:
        lon = lon_min
        while lon < lon_max:
            cells.append({
                "cell_id":    f"c{cid:04d}",
                "lat_min":    round(lat, 6),
                "lat_max":    round(min(lat + d_lat, lat_max), 6),
                "lon_min":    round(lon, 6),
                "lon_max":    round(min(lon + d_lon, lon_max), 6),
                "lat_center": round(lat + d_lat / 2, 6),
                "lon_center": round(lon + d_lon / 2, 6),
            })
            cid += 1; lon += d_lon
        lat += d_lat
    log.info(f"  Grid: {len(cells)} cells ({cell_km} km)")
    return cells

def cells_to_ee_fc(cells):
    return ee.FeatureCollection([
        ee.Feature(
            ee.Geometry.Rectangle([c["lon_min"], c["lat_min"],
                                   c["lon_max"], c["lat_max"]]),
            {"cell_id": c["cell_id"],
             "lat_center": c["lat_center"],
             "lon_center": c["lon_center"]})
        for c in cells])

def save_grid_geojson(cells, out_dir, region_meta):
    gj = {"type": "FeatureCollection", "region": region_meta, "features": [
        {"type": "Feature",
         "properties": {k: v for k, v in c.items()},
         "geometry": {"type": "Polygon", "coordinates": [[
             [c["lon_min"], c["lat_min"]], [c["lon_max"], c["lat_min"]],
             [c["lon_max"], c["lat_max"]], [c["lon_min"], c["lat_max"]],
             [c["lon_min"], c["lat_min"]]]]}}
        for c in cells]}
    p = Path(out_dir) / "grid.geojson"
    with open(p, "w") as f: json.dump(gj, f, indent=2)
    log.info(f"  Grid saved: {p} ({len(cells)} cells)")

# ── Date helpers ───────────────────────────────────────────────────────────
def date_chunks(start_str, end_str, months):
    start = datetime.strptime(start_str, "%Y-%m-%d")
    end   = datetime.strptime(end_str,   "%Y-%m-%d")
    cur   = start
    while cur < end:
        m   = cur.month - 1 + months
        nxt = datetime(cur.year + m // 12, m % 12 + 1, 1)
        nxt = min(nxt, end)
        yield cur.strftime("%Y-%m-%d"), nxt.strftime("%Y-%m-%d")
        cur = nxt

def sub_windows(cs, ce, resolution):
    """Generate (start, end) pairs for sub-chunk windows."""
    start = datetime.strptime(cs, "%Y-%m-%d")
    end   = datetime.strptime(ce, "%Y-%m-%d")
    if resolution == "period":
        yield cs, ce; return
    step = {"daily": 1, "3day": 3, "8day": 8, "16day": 16, "scene": 1}[resolution]
    cur = start
    while cur < end:
        nxt = min(cur + timedelta(days=step), end)
        yield cur.strftime("%Y-%m-%d"), nxt.strftime("%Y-%m-%d")
        cur = nxt

# ── File helpers ───────────────────────────────────────────────────────────
def chunk_path(out_dir, var_id, cs, ce):
    return Path(out_dir) / var_id / f"{var_id}_{cs}_{ce}.csv"

def full_path(out_dir, var_id, cell_id):
    return Path(out_dir) / var_id / f"{var_id}_{cell_id}_full.csv"

def chunk_done(out_dir, var_id, cs, ce):
    skip = cfg.get("EXPORT", {}).get("skip_existing", True)
    return skip and chunk_path(out_dir, var_id, cs, ce).exists()

# ── Collection builders ────────────────────────────────────────────────────
def col_s2_indices(s, e, aoi):
    def add(img):
        ndvi  = img.normalizedDifference(["B8",  "B4"]).rename("NDVI")
        ndwi  = img.normalizedDifference(["B3",  "B8"]).rename("NDWI")
        ndbi  = img.normalizedDifference(["B11", "B8"]).rename("NDBI")
        cloud = ee.Image.constant(
            img.getNumber("CLOUDY_PIXEL_PERCENTAGE")).toFloat().rename("cloud_pct")
        return img.addBands([ndvi, ndwi, ndbi, cloud])
    return (ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
              .filterBounds(aoi).filterDate(s, e).map(add))

def col_ndvi(s, e, g): return col_s2_indices(s, e, g).select(["NDVI", "cloud_pct"])
def col_ndwi(s, e, g): return col_s2_indices(s, e, g).select(["NDWI", "cloud_pct"])
def col_ndbi(s, e, g): return col_s2_indices(s, e, g).select(["NDBI", "cloud_pct"])

def col_evi(s, e, g):
    return (ee.ImageCollection("MODIS/061/MOD13Q1")
              .filterBounds(g).filterDate(s, e).select("EVI")
              .map(lambda i: i.multiply(0.0001)
                              .copyProperties(i, ["system:time_start"])))

def col_lst(s, e, g):
    return (ee.ImageCollection("MODIS/061/MOD11A1")
              .filterBounds(g).filterDate(s, e).select("LST_Day_1km")
              .map(lambda i: i.multiply(0.02).subtract(273.15).rename("LST")
                              .copyProperties(i, ["system:time_start"])))

def col_chirps(s, e, g):
    return (ee.ImageCollection("UCSB-CHG/CHIRPS/DAILY")
              .filterBounds(g).filterDate(s, e).select("precipitation"))

def col_smap(s, e, g):
    return (ee.ImageCollection("NASA/SMAP/SPL4SMGP/008")
              .filterBounds(g).filterDate(s, e).select("sm_surface"))

def _era5_daily(band, rename, transform, s, e, g):
    """Generic ERA5 daily aggregator."""
    def daily(date):
        d  = ee.Date(date)
        dc = (ee.ImageCollection("ECMWF/ERA5_LAND/HOURLY")
                .filterBounds(g)
                .filterDate(d, d.advance(1, "day"))
                .select(band if isinstance(band, list) else [band]))
        img = transform(dc)
        return img.set("system:time_start", d.millis())
    days  = ee.List.sequence(0, ee.Date(e).difference(ee.Date(s), "day").subtract(1))
    dates = days.map(lambda n: ee.Date(s).advance(n, "day").format("YYYY-MM-dd"))
    return ee.ImageCollection(dates.map(daily))

def col_era5_temp(s, e, g):
    return _era5_daily(
        "temperature_2m", "air_temp_C",
        lambda dc: dc.mean().subtract(273.15).rename("air_temp_C"),
        s, e, g)

def col_era5_humidity(s, e, g):
    def rh(dc):
        t  = dc.select("temperature_2m").mean()
        td = dc.select("dewpoint_temperature_2m").mean()
        return (ee.Image(100)
                  .multiply(ee.Image(17.625).multiply(td.subtract(273.15))
                                            .divide(ee.Image(243.04).add(td.subtract(273.15))).exp())
                  .divide(ee.Image(17.625).multiply(t.subtract(273.15))
                                          .divide(ee.Image(243.04).add(t.subtract(273.15))).exp())
                  .rename("RH"))
    return _era5_daily(
        ["temperature_2m", "dewpoint_temperature_2m"], "RH", rh, s, e, g)

def col_era5_wind(s, e, g):
    def wspd(dc):
        u = dc.select("u_component_of_wind_10m").mean()
        v = dc.select("v_component_of_wind_10m").mean()
        return u.pow(2).add(v.pow(2)).sqrt().rename("wind_speed_ms")
    return _era5_daily(
        ["u_component_of_wind_10m", "v_component_of_wind_10m"], "wind_speed_ms", wspd, s, e, g)

def col_no2(s, e, g):
    return (ee.ImageCollection("COPERNICUS/S5P/NRTI/L3_NO2")
              .filterBounds(g).filterDate(s, e)
              .select("NO2_column_number_density")
              .map(lambda i: i.multiply(1e6).rename("NO2_umol_m2")
                              .copyProperties(i, ["system:time_start"])))

def col_co(s, e, g):
    return (ee.ImageCollection("COPERNICUS/S5P/NRTI/L3_CO")
              .filterBounds(g).filterDate(s, e)
              .select("CO_column_number_density")
              .map(lambda i: i.multiply(1000).rename("CO_mmol_m2")
                              .copyProperties(i, ["system:time_start"])))

def col_firms(s, e, g):
    return (ee.ImageCollection("FIRMS")
              .filterBounds(g).filterDate(s, e).select("T21"))

def col_lulc(s, e, g):
    modal = (ee.ImageCollection("GOOGLE/DYNAMICWORLD/V1")
               .filterBounds(g).filterDate(s, e).select("label")
               .reduce(ee.Reducer.mode()).rename("label"))
    return ee.ImageCollection(
        [modal.set("system:time_start", ee.Date(s).millis())])

def col_albedo(s, e, g):
    return (ee.ImageCollection("MODIS/061/MCD43A3")
              .filterBounds(g).filterDate(s, e).select("Albedo_BSA_shortwave")
              .map(lambda i: i.multiply(0.001).rename("albedo")
                              .copyProperties(i, ["system:time_start"])))

def col_lai(s, e, g):
    return (ee.ImageCollection("MODIS/061/MOD15A2H")
              .filterBounds(g).filterDate(s, e).select("Lai_500m")
              .map(lambda i: i.multiply(0.1).rename("LAI")
                              .copyProperties(i, ["system:time_start"])))

def col_snow(s, e, g):
    return (ee.ImageCollection("MODIS/061/MOD10A1")
              .filterBounds(g).filterDate(s, e).select("NDSI_Snow_Cover")
              .map(lambda i: i.rename("snow_cover")
                              .copyProperties(i, ["system:time_start"])))

def col_et(s, e, g):
    return (ee.ImageCollection("MODIS/061/MOD16A2")
              .filterBounds(g).filterDate(s, e).select("ET")
              .map(lambda i: i.multiply(0.1).rename("ET_mm")
                              .copyProperties(i, ["system:time_start"])))

def col_aer(s, e, g):
    return (ee.ImageCollection("COPERNICUS/S5P/NRTI/L3_AER_AI")
              .filterBounds(g).filterDate(s, e)
              .select("absorbing_aerosol_index")
              .map(lambda i: i.copyProperties(i, ["system:time_start"])))

def col_o3(s, e, g):
    return (ee.ImageCollection("COPERNICUS/S5P/NRTI/L3_O3")
              .filterBounds(g).filterDate(s, e)
              .select("O3_column_number_density")
              .map(lambda i: i.multiply(1000).rename("O3_mmol_m2")
                              .copyProperties(i, ["system:time_start"])))

COLLECTIONS = {
    "ndvi": col_ndvi, "evi": col_evi, "ndwi": col_ndwi, "ndbi": col_ndbi,
    "lst": col_lst, "precipitation": col_chirps, "soil_moisture": col_smap,
    "air_temp": col_era5_temp, "humidity": col_era5_humidity,
    "wind_speed": col_era5_wind, "no2": col_no2, "co": col_co,
    "fires": col_firms, "lulc": col_lulc, "albedo": col_albedo,
    "lai": col_lai, "snow_cover": col_snow, "evapotranspiration": col_et,
    "aerosol": col_aer, "o3": col_o3,
}

# ── Core reduction — ONE reduceRegions call per sub-window ─────────────────
def export_chunk(var_id, cells, cells_fc, aoi_geom, cs, ce, out_dir, region_scale, crs, n_cells=None):
    """
    For each temporal sub-window within [cs, ce]:
      1. Collapse the sub-window's images into a single composite (mean/mode)
         entirely server-side.
      2. Call reduceRegions ONCE over ALL cells.
      3. Tag each row with the sub-window date.

    Total getInfo() calls = number of sub-windows (≤ chunk_days / step).
    No per-cell loops. No batching needed.
    """
    retries    = cfg["CHUNKING"]["max_retries"]
    wait       = cfg["CHUNKING"]["retry_wait_s"]
    out        = chunk_path(out_dir, var_id, cs, ce)
    chunk_m    = CHUNK_MONTHS.get(var_id, DEFAULT_CHUNK_MONTHS)
    resolution = effective_resolution(var_id, n_cells or len(cells), chunk_m)
    scale      = effective_scale(var_id, region_scale)
    reducer    = ee.Reducer.mode() if var_id == "lulc" else ee.Reducer.mean()
    is_scene   = resolution == "scene"

    for attempt in range(1, retries + 1):
        try:
            all_rows = []
            windows  = list(sub_windows(cs, ce, resolution))

            def fetch_window(ws_we):
                ws, we = ws_we
                col = COLLECTIONS[var_id](ws, we, aoi_geom)
                rows = []
                if is_scene:
                    n_imgs = col.size().getInfo()
                    if n_imgs == 0:
                        return rows  # no S2 acquisitions in this window
                    img_list = col.toList(n_imgs)
                    for idx in range(n_imgs):
                        img  = ee.Image(img_list.get(idx))
                        date = ee.Date(img.get("system:time_start")).format("YYYY-MM-dd").getInfo()
                        result = img.reduceRegions(
                            collection=cells_fc, reducer=reducer,
                            scale=scale, crs=crs).getInfo()
                        for feat in result.get("features", []):
                            p = feat.get("properties", {})
                            if p.get("cell_id"):
                                row = {k: v for k, v in p.items() if v is not None}
                                row["date"] = date
                                rows.append(row)
                else:
                    if col.size().getInfo() == 0:
                        return rows  # no data in this window
                    composite = col.mean() if var_id != "lulc" else col.reduce(ee.Reducer.mode())
                    if var_id == "lulc":
                        composite = composite.rename("label")
                    result = composite.reduceRegions(
                        collection=cells_fc, reducer=reducer,
                        scale=scale, crs=crs).getInfo()
                    for feat in result.get("features", []):
                        p = feat.get("properties", {})
                        if p.get("cell_id"):
                            row = {k: v for k, v in p.items() if v is not None}
                            row["date"] = ws
                            rows.append(row)
                return rows

            # Run sub-windows in parallel (GEE supports ~10 concurrent requests)
            max_workers = min(10, len(windows))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(fetch_window, w): w for w in windows}
                for future in as_completed(futures):
                    all_rows.extend(future.result())

            df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame()

            if not df.empty:
                df.to_csv(out, index=False)
                log.info(
                    f"  {var_id} {cs}->{ce}: {len(df)} rows "
                    f"({df['cell_id'].nunique()} cells, "
                    f"{df['date'].nunique()} dates, "
                    f"{len(windows)} windows)")
            else:
                log.warning(f"  {var_id} {cs}->{ce}: no data")
                pd.DataFrame().to_csv(out, index=False)
            return True

        except Exception as exc:
            log.error(f"  attempt {attempt}/{retries}: {exc}")
            if attempt < retries:
                time.sleep(wait)
    return False

# ── Merge chunks → per-cell full CSV ───────────────────────────────────────
def merge_to_cell_files(var_id, cell_ids, out_dir):
    var_dir = Path(out_dir) / var_id
    dfs = [pd.read_csv(f)
           for f in sorted(var_dir.glob(f"{var_id}_2*.csv"))
           if f.stat().st_size > 10]
    if not dfs:
        return
    all_data = pd.concat(dfs).drop_duplicates(subset=["date", "cell_id"])
    for cell_id in cell_ids:
        cdf = (all_data[all_data["cell_id"] == cell_id]
                       .sort_values("date").reset_index(drop=True))
        if not cdf.empty:
            cdf.to_csv(full_path(out_dir, var_id, cell_id), index=False)
    log.info(f"  merged {var_id}: {len(all_data)} rows → {len(cell_ids)} cell files")

# ── Process one region ─────────────────────────────────────────────────────
def process_region(region_cfg, enabled_vars):
    name    = region_cfg["name"]
    out_dir = region_cfg["output_dir"]
    cell_km = region_cfg["cell_size_km"]
    scale   = region_cfg["scale_m"]  # region_scale passed to export_chunk
    crs     = cfg.get("EXPORT", {}).get("crs", "EPSG:4326")
    aoi     = region_cfg["aoi"]

    log.info(f"\n{'='*60}")
    log.info(f"Region: {name}  |  cell={cell_km}km  |  scale={scale}m")

    Path(out_dir).mkdir(parents=True, exist_ok=True)
    cells    = build_grid(aoi, cell_km)
    cell_ids = [c["cell_id"] for c in cells]
    log.info(f"  Building EE FeatureCollection ({len(cells)} cells)...")
    import time as _t; _t0 = _t.time()
    cells_fc = cells_to_ee_fc(cells)
    log.info(f"  cells_fc built in {_t.time()-_t0:.1f}s")
    aoi_geom = ee.FeatureCollection(aoi).geometry()
    log.info(f"  aoi_geom built in {_t.time()-_t0:.1f}s total")

    save_grid_geojson(cells, out_dir, {
        "name":     name,
        "label_it": region_cfg.get("label_it", name),
        "label_en": region_cfg.get("label_en", name),
    })

    total_chunks = sum(
        len(list(date_chunks(cfg["TIME"]["start"], cfg["TIME"]["end"],
                             CHUNK_MONTHS.get(v, DEFAULT_CHUNK_MONTHS))))
        for v in enabled_vars)
    log.info(f"Variables: {list(enabled_vars.keys())}  |  Chunks: {total_chunks}")

    if args.dry_run:
        for var_id in enabled_vars:
            m = CHUNK_MONTHS.get(var_id, DEFAULT_CHUNK_MONTHS)
            for cs, ce in date_chunks(cfg["TIME"]["start"], cfg["TIME"]["end"], m):
                flag = "done" if chunk_done(out_dir, var_id, cs, ce) else "todo"
                res  = VAR_RESOLUTION.get(var_id, "daily")
                nwin = len(list(sub_windows(cs, ce, res)))
                print(f"  [{flag}] {name:10s} {var_id:22s} {cs} -> {ce}  ({nwin} windows)")
        return

    failed = []
    with tqdm(total=total_chunks, unit="chunk", desc=name) as pbar:
        for var_id in enabled_vars:
            (Path(out_dir) / var_id).mkdir(parents=True, exist_ok=True)
            months = CHUNK_MONTHS.get(var_id, DEFAULT_CHUNK_MONTHS)
            for cs, ce in date_chunks(cfg["TIME"]["start"], cfg["TIME"]["end"], months):
                pbar.set_description(f"{name}/{var_id} {cs[:7]}")
                if chunk_done(out_dir, var_id, cs, ce):
                    pbar.update(1); continue
                ok = export_chunk(var_id, cells, cells_fc, aoi_geom,
                                  cs, ce, out_dir, scale, crs, len(cells))
                if not ok:
                    failed.append((name, var_id, cs, ce))
                pbar.update(1)
                time.sleep(cfg["CHUNKING"]["pause_between_chunks_s"])
            merge_to_cell_files(var_id, cell_ids, out_dir)

    meta = {
        "last_update":  datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "region":       name,
        "label_it":     region_cfg.get("label_it", name),
        "label_en":     region_cfg.get("label_en", name),
        "time_start":   cfg["TIME"]["start"],
        "time_end":     cfg["TIME"]["end"],
        "scale_m":      scale,
        "cell_size_km": cell_km,
        "n_cells":      len(cells),
        "variables":    list(enabled_vars.keys()),
    }
    with open(Path(out_dir) / "metadata.json", "w") as f:
        json.dump(meta, f, indent=2)

    if failed:
        log.warning(f"Failed chunks: {failed}")
    else:
        log.info(f"Region {name} complete.")

# ── Main ───────────────────────────────────────────────────────────────────
def main():
    regions = cfg.get("REGIONS", [])
    if args.regions:
        regions = [r for r in regions if r["name"] in args.regions]
    if not regions:
        log.error("No regions in config.yaml"); sys.exit(1)

    enabled = {k: v for k, v in cfg["VARIABLES"].items() if v.get("enabled", True)}
    if args.vars:
        enabled = {k: v for k, v in enabled.items() if k in args.vars}

    for r in regions:
        process_region(r, enabled)

if __name__ == "__main__":
    main()
# MappySAT

An interactive web dashboard for environmental monitoring using Google Earth Engine data. Displays time series and spatial data for 19 variables across a configurable multi-region grid.

Static HTML site on GitHub Pages. Data exported locally via Python and committed to the repository.

## Project structure

    .
    ├── index.html          Web dashboard (map + charts + sidebar)
    ├── config.yaml         Multi-region export settings
    ├── secrets.yaml        GEE project ID — not committed
    ├── gee_export.py       Download script
    ├── data/
    │   ├── sannio/         0.5 km grid, scale 500 m
    │   │   ├── grid.geojson
    │   │   ├── metadata.json
    │   │   └── ndvi/
    │   │       ├── ndvi_c000_full.csv    ← committed, read by dashboard
    │   │       └── ndvi_2025-01-01_2025-02-01.csv  ← not committed
    │   └── italia/         25 km grid, scale 25000 m
    │       └── ...
    ├── logs/               Export logs — not committed
    ├── _config.yml         Jekyll config
    └── .gitignore

## Setup

```bash
pip install earthengine-api pyyaml pandas tqdm
earthengine authenticate
```

Create `secrets.yaml`:
```yaml
GEE:
  project: "your-project-id"
```

Register at https://earthengine.google.com/register if needed.

## Downloading data

```bash
python gee_export.py                          # all regions, all variables
python gee_export.py --region sannio          # single region
python gee_export.py --var ndvi --var lst      # specific variables
python gee_export.py --dry-run                 # preview without downloading
```

Chunks already on disk are skipped automatically (`skip_existing: true`). To re-merge existing chunks without re-downloading:

```bash
python3 - << 'PYEOF'
import pandas as pd
from pathlib import Path

for region_dir in Path('data').iterdir():
    if not region_dir.is_dir(): continue
    for var_dir in region_dir.iterdir():
        if not var_dir.is_dir(): continue
        dfs = [pd.read_csv(f) for f in sorted(var_dir.glob(f'{var_dir.name}_2*.csv')) if f.stat().st_size > 10]
        if not dfs: continue
        all_data = pd.concat(dfs).drop_duplicates(subset=['date','cell_id'])
        for cell_id, g in all_data.groupby('cell_id'):
            g.sort_values('date').to_csv(var_dir / f'{var_dir.name}_{cell_id}_full.csv', index=False)
        print(f'{region_dir.name}/{var_dir.name}: {len(all_data)} rows')
PYEOF
```

## Configuration

`config.yaml` defines one or more regions under `REGIONS`, each with its own AOI, cell size, scale, and output directory. The time window and variables are shared across all regions.

Key parameters per region:

| Parameter      | Description                                      |
|----------------|--------------------------------------------------|
| `cell_size_km` | Grid cell size in km. Do not change after downloading. |
| `scale_m`      | GEE reduceRegions scale. Use dataset native resolution or larger. |
| `output_dir`   | Where data is stored. Move existing data here to preserve it. |

Shared parameters:

| Parameter               | Description                              |
|-------------------------|------------------------------------------|
| `TIME.start / end`      | Analysis period                          |
| `CHUNKING.chunk_months` | Time window per GEE request              |

**Do not change `cell_size_km` or `chunk_months` after downloading** — cell IDs and chunk filenames are derived from these values.

## Multi-region workflow

To add a new region without losing existing data:
1. Add a new entry under `REGIONS` in `config.yaml`
2. Set `output_dir` to a new path (e.g. `./data/napoli`)
3. Run `python gee_export.py --region napoli`

Existing regions are unaffected. The dashboard dropdown switches between regions automatically.

## Deploying to GitHub Pages

```bash
git init
git remote add origin https://github.com/YOUR_NAME/YOUR_REPO.git
git add .
git commit -m "initial commit"
git push -u origin main
```

Enable Pages under Settings → Pages → Branch: main / root.

Update data:
```bash
git add data/
git commit -m "data update $(date +%Y-%m-%d)"
git push
```

**Note:** only `_full.csv` files are committed (chunks excluded via `.gitignore`). Keep total repo size under 1 GB.

## Local development

```bash
jekyll serve --livereload   # live reload on file save
# or
python -m http.server 8000
```

Open http://localhost:4000 or http://localhost:8000. Must be served over HTTP — `file://` does not work.

## Variables

| ID                  | Source                  | Temporal resolution     | Unit          |
|---------------------|-------------------------|-------------------------|---------------|
| ndvi                | Sentinel-2 SR           | per acquisition (~5d)   | [-1, 1]       |
| ndwi                | Sentinel-2 SR           | per acquisition (~5d)   | [-1, 1]       |
| ndbi                | Sentinel-2 SR           | per acquisition (~5d)   | [-1, 1]       |
| evi                 | MODIS MOD13Q1           | 16 days                 | [-1, 1]       |
| lst                 | MODIS MOD11A1           | daily                   | °C            |
| precipitation       | CHIRPS                  | daily                   | mm            |
| soil_moisture       | NASA SMAP SPL4SMGP/008  | ~3 days                 | m³/m³         |
| air_temp            | ERA5-Land               | daily mean              | °C            |
| humidity            | ERA5-Land               | daily mean              | %             |
| wind_speed          | ERA5-Land               | daily mean              | m/s           |
| no2                 | Sentinel-5P TROPOMI     | daily                   | µmol/m² *     |
| co                  | Sentinel-5P TROPOMI     | daily                   | mmol/m² *     |
| fires               | MODIS FIRMS             | daily                   | count         |
| lulc                | Google Dynamic World V1 | near-real-time          | class (0–8)   |
| albedo              | MODIS MCD43A3           | 8 days                  | [0, 1]        |
| lai                 | MODIS MOD15A2H          | 8 days                  | m²/m²         |
| evapotranspiration  | MODIS MOD16A2           | 8 days                  | mm            |
| aerosol             | Sentinel-5P TROPOMI     | daily                   | AAI           |
| o3                  | Sentinel-5P TROPOMI     | daily                   | mmol/m² * |

\* Unit conversion applied at export time (mol/m² × 1e6 for NO₂, × 1000 for CO and O₃).

**Sentinel-2** (ndvi, ndwi, ndbi): cloud filtering disabled. `cloud_pct` column records per-scene cloud cover for downstream filtering.

**ERA5-Land** (air_temp, humidity, wind_speed): aggregated to daily means server-side before download.

**Dynamic World** (lulc): 9 classes — water, trees, grass, flooded veg., crops, shrub, built-up, bare, snow. Exported as modal class over the chunk period.

## .gitignore

    secrets.yaml
    data/*/*_2*.csv
    _site/
    .jekyll-cache/
    logs/
    __pycache__/
    *.pyc
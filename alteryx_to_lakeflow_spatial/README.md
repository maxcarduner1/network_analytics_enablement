# LoRa Network Analytics — Alteryx → Databricks Demo

Rebuilds an Alteryx spatial workflow (cell sites → which Top-100 Small/Rural County Market they fall in) on Databricks, with the day-to-day editing surface as visual and familiar as Alteryx Designer.

## Architecture

```
   Volume files                Bronze ingestion              Lakeflow Designer pipeline
 (CSV, MapInfo TAB)   ──▶   (00_bronze_ingestion.py,  ──▶   (01_dlt_lakeflow_pipeline.sql,
                              run once on serverless)         visual canvas)
```

**Why split?** MapInfo `.TAB` is proprietary and needs `geopandas` to parse. Lakeflow Designer doesn't ingest TAB directly, so we handle it once in a notebook. Every other step lives on the Designer canvas.

## Files

| File | What it is |
|---|---|
| `00_bronze_ingestion.py` | One-time ingest: CSV + MapInfo TAB → bronze Delta tables. Parameterized via widgets. |
| `01_dlt_lakeflow_pipeline.sql` | Lakeflow Declarative Pipeline. Open this in Designer for the visual canvas. |
| `WALKTHROUGH.md` | Step-by-step walkthrough — Alteryx↔Designer node mapping, how to edit, schedule, operationalize. |

## Quick start

### 1. Stage the source files in a Unity Catalog Volume

Upload these into any UC Volume:
- `site_lat_long.csv`
- `Top100_SmallRural_County_Markets_2021.TAB` (+ DAT/MAP/ID/IND sidecars)

### 2. Run `00_bronze_ingestion.py`

Open the notebook, set the three widgets, attach to serverless, Run All:

| Widget | Default |
|---|---|
| `catalog` | `cmegdemos_catalog` |
| `schema` | `network_analytics_enablement` |
| `volume_path` | `/Volumes/cmegdemos_catalog/network_analytics_enablement/lora` |

This produces two Delta tables: `sites_bronze` (1,000 rows) and `county_markets_bronze` (2,179 rows).

### 3. Create the Lakeflow pipeline from `01_dlt_lakeflow_pipeline.sql`

**Option A — UI (build in Designer):**
1. Workflows → Pipelines → **Create pipeline**.
2. Pick **Serverless**, choose your destination catalog + schema.
3. Under **Source code → Notebook libraries**, add `01_dlt_lakeflow_pipeline.sql` from the workspace.
4. Under **Advanced → Configuration**, add two keys:
   - `bronze_catalog` → same catalog you used in step 2
   - `bronze_schema`  → same schema you used in step 2
5. Save. Click **Open in Designer** to see the 5-node visual canvas. Click **Start** to run.

**Option B — CLI:**
```bash
databricks pipelines create --json '{
  "name": "lora_network_analytics",
  "serverless": true,
  "catalog": "YOUR_CATALOG",
  "schema":  "YOUR_SCHEMA",
  "libraries": [{"notebook": {"path": "/path/to/01_dlt_lakeflow_pipeline"}}],
  "configuration": {
    "bronze_catalog": "YOUR_BRONZE_CATALOG",
    "bronze_schema":  "YOUR_BRONZE_SCHEMA"
  },
  "channel": "CURRENT",
  "edition": "ADVANCED",
  "development": true,
  "photon": true
}'
```

Then trigger an update with `databricks pipelines start-update <pipeline_id>`.

## Alteryx ↔ Databricks node mapping

| Alteryx tool | Designer node | Implementation |
|---|---|---|
| Input Data (CSV) | (bronze notebook) | `spark.read.csv` |
| Input Data (MapInfo TAB) | (bronze notebook) | `geopandas.read_file` |
| Formula `IIF(isnull(...))` | `sites_silver` | `COALESCE` |
| Create Points | inline in queries | `ST_Point(lon, lat)` |
| Spatial Match (within) | `sites_in_market` | `ST_Contains(polygon, point)` |
| Find Nearest | `sites_nearest_market` | `ST_DistanceSphere` + `ROW_NUMBER()` |
| Browse / Output | `site_market_enriched` | gold Delta table |

## Why these design choices

- **Geometry stored as WKT strings** (not native `GEOMETRY` type) — portable across DBR 16.4 LTS and up, including serverless. To switch to native `GEOMETRY` on DBR 17+, wrap with `ST_GeomFromWKT(...)` and cast the column.
- **Polygons simplified to ~0.001° (~100 m)** during ingest — prevents Spark row-buffer overflow on multi-million-vertex MapInfo polygons. Tune `tolerance` higher/lower as needed.
- **Single install cell** `geopandas pyogrio` + `%restart_python` — works on serverless without breaking pandas.

See `WALKTHROUGH.md` for the full step-by-step.

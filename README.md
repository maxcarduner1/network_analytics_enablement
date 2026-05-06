# Network Analytics Enablement

## Purpose

This repo demonstrates a pattern for **instruction-driven notebook development with Genie Code** (the Databricks AI assistant). Each notebook contains only markdown cells with natural-language instructions describing what to build. Genie Code reads these instructions, writes the implementation code, executes it, and iterates until the pipeline is complete.

The goal is to show that a data engineer or analyst can describe _what_ they want in plain English and let the assistant handle the _how_ — including reading unfamiliar file formats, choosing the right spatial functions, and debugging errors along the way.

## Data Flow

The pipeline analyzes **T-Mobile 5G NR coverage of buildings in downtown Seattle** by combining three public geospatial datasets:

```
┌─────────────────────────────────────────────────────────┐
│  Raw Data (Unity Catalog Volume: raw_data)              │
│                                                         │
│  bdc_*…zip   Washington.zip   310.csv.gz @ volume root       │
│  (FCC zip)   (Shapefiles)      + cell_towers/*.csv.gz (SDP)    │
│                               + kpis/*.csv.gz demand/*.csv.gz │
│                               (ops demo Auto Loader)           │
└────────────┬──────────────┬───────────────┬─────────────┘
             │ 01_Ingest    │               │
             ▼              ▼               ▼
┌────────────────┐ ┌────────────────┐ ┌────────────────┐
│fcc_bdc_h3      │ │building        │ │cell_towers     │
│_seattle        │ │_footprints     │ │                │
│                │ │                │ │                │
│ 13,966 H3 hex │ │ 442,320 bldgs  │ │ 2,312 T-Mobile │
│ res-9 cells    │ │ geometry(4326) │ │ towers w/ POINT│
└───────┬────────┘ └───────┬────────┘ └───────┬────────┘
        │    02_Analysis   │                  │
        │    ┌─────────────┤                  │
        ▼    ▼             │                  ▼
   H3 join on          Centroid +      Haversine nearest
   h3_res9_id          bbox filter     tower cross-join
        │                  │                  │
        └──────────┬───────┘──────────────────┘
                   ▼
┌──────────────────────────────────────────────┐
│  downtown_seattle_building_coverage          │
│  1,565 buildings × 16 columns                │
│  (coverage speed, nearest tower, distance)   │
└──────────────────────────────────────────────┘
                   │
                   ▼
        Folium interactive heatmap
        + distance vs. signal analysis
```

**SDP bundle (`network_analytics_pipeline/`):** OpenCellID flows through **`raw_data/cell_towers/*.csv.gz`** via Auto Loader into **`bronze_cell_towers`** (streaming Delta). Keep an optional canonical **`310.csv.gz`** at the volume root for notebooks and for [demo_generate_cell_towers_shard.ipynb](network_analytics_pipeline/notebooks/demo_generate_cell_towers_shard.ipynb), which writes random sample shards into **`cell_towers/`**. Synthetic ops telemetry shards land under **`raw_data/kpis/`** and **`raw_data/demand/`** (see demo notebooks below) for **`ops_app_network_analytics_pipeline`**. See [network_analytics_pipeline/README.md](network_analytics_pipeline/README.md).

## Notebooks

### `01_Ingest.ipynb` — Raw File Ingestion

Reads three compressed/archived geospatial files from a Unity Catalog volume and produces clean Delta tables filtered to the Seattle metro area.

| Source File | Format | Target Table | Rows | Approach |
| --- | --- | --- | --- | --- |
| `bdc_53_5GNR_mobile_broadband_h3_J25_14apr2026.zip` | GeoPackage (.gpkg) in zip | `fcc_bdc_h3_seattle` | 13,966 | sqlite3 rtree pre-filter + H3 center bbox |
| `Washington.zip` | Shapefile (.shp) in zip | `building_footprints` | 442,320 | geopandas read, WKT to native geometry(4326) |
| `310.csv.gz` at volume root (typical notebook ingest) | Headerless gzipped CSV | `cell_towers` | 2,312 | Spark CSV, filter MNC 260 (T-Mobile) + bbox |

### `02_Analysis.ipynb` — Downtown Seattle Coverage Estimation

Joins the three ingested tables to estimate 5G NR coverage per building in downtown Seattle.

| Step | What it does | Key functions used |
| --- | --- | --- |
| 1 | Filter buildings to downtown bbox, assign H3 res-9 index | `ST_Centroid`, `ST_X`, `ST_Y`, `h3_longlatash3string` |
| 2 | LEFT JOIN with FCC BDC coverage on `h3_res9_id` | `MAX(mindown)`, `COUNT(DISTINCT fid)` |
| 3 | Find nearest cell tower per building | Haversine formula, `CROSS JOIN` + `ROW_NUMBER` |
| 4 | Save final table | `downtown_seattle_building_coverage` (1,565 rows) |
| 5 | Interactive heatmap | `folium` with tower markers and coverage heat layer |
| 6 | Distance vs. signal strength analysis | Bucketed aggregation by distance to nearest tower |

## Catalog & Schema

**Databricks bundle default:** **`cmegdemos_catalog.network_analytics_enablement`** (override via `catalog` / `schema` in [network_analytics_pipeline/databricks.yml](network_analytics_pipeline/databricks.yml)). The root notebooks **`01_Ingest.ipynb`** and **`02_Analysis.ipynb`** parameterize `CATALOG` and `SCHEMA` in their setup cells — keep them aligned with the bundle target when comparing notebook outputs to SDP tables.

SDP publishes **`bronze_*` / `silver_*` / `gold_*`** (and **`ops_app_*`** for the ops pipeline) so names do not collide with legacy notebook table names such as `fcc_bdc_h3_seattle` or `downtown_seattle_building_coverage`.

| Table | Description |
| --- | --- |
| `fcc_bdc_h3_seattle` | FCC Broadband Data Collection H3 res-9 hexagons for 5G NR in Seattle |
| `building_footprints` | Microsoft building footprint polygons (geometry + height) |
| `cell_towers` | OpenCellID T-Mobile tower locations with POINT geometry |
| `downtown_seattle_building_coverage` | Final analysis notebook output: building + coverage + nearest tower |

Tables prefixed **`bronze_` / `silver_` / `gold_`** and **`ops_app_*`** come from the [SDP bundles](network_analytics_pipeline/README.md); the richest merged view for demos is **`ops_app_gold_downtown_building_coverage`** (coverage geometry + synthetic tower KPIs + building demand).

### SDP demo notebooks (volume shards)

| Notebook | Writes | Consumed by |
| --- | --- | --- |
| [demo_generate_cell_towers_shard.ipynb](network_analytics_pipeline/notebooks/demo_generate_cell_towers_shard.ipynb) | `raw_data/cell_towers/*.csv.gz` | `network_analytics_pipeline` → `bronze_cell_towers` |
| [demo_generate_ops_app_kpis_shard.ipynb](network_analytics_pipeline/notebooks/demo_generate_ops_app_kpis_shard.ipynb) | `raw_data/kpis/*.csv.gz` | `ops_app_network_analytics_pipeline` → `ops_app_bronze_tower_hourly_kpis` |
| [demo_generate_ops_app_demand_shard.ipynb](network_analytics_pipeline/notebooks/demo_generate_ops_app_demand_shard.ipynb) | `raw_data/demand/*.csv.gz` | `ops_app_network_analytics_pipeline` → `ops_app_bronze_building_hourly_demand` |

KPI and demand generators sample **`310.csv.gz`** for variability and attach **real `tower_id`** / **`building_id`** from **`silver_tmobile_towers_seattle`** and **`gold_downtown_building_coverage`** after the base pipeline has populated those tables.

## How to Use

1. Open **`01_Ingest.ipynb`** in the Databricks notebook editor
2. Attach serverless compute (or a personal all-purpose cluster)
3. Run all cells — the markdown instructions at the top describe the intent; the code cells below were generated by Genie Code
4. Open **`02_Analysis.ipynb`** and run all cells — it reads from the tables created in step 3
5. The interactive heatmap in Step 5 is scrollable/zoomable and shows tower locations as red markers

To re-run from scratch against a different catalog/schema, update the `CATALOG` and `SCHEMA` variables in the setup cell of each notebook.

## Medallion pipeline (SDP)

The same transformations are also available as bundle-deployable **Lakeflow Spark Declarative Pipelines** (**SDP**) under [`network_analytics_pipeline/`](network_analytics_pipeline/). Product documentation: [Lakeflow Spark Declarative Pipelines](https://docs.databricks.com/aws/en/ldp).

| Bundle resource | Role |
| --- | --- |
| **`network_analytics_pipeline`** | Main medallion: FCC + buildings + OpenCellID (**Auto Loader** from `raw_data/cell_towers/`) → **`gold_downtown_building_coverage`** and **`gold_coverage_by_distance_bucket`**. |
| **`ops_app_network_analytics_pipeline`** *(optional)* | **Auto Loader** from **`raw_data/kpis/`** and **`raw_data/demand/`** → bronze → silver “latest per tower/building” → **`ops_app_gold_downtown_building_coverage`** (**LEFT JOIN** baseline **`gold_downtown_building_coverage`** with demand + nearest-tower KPIs). Uses **`ops_app_*`** table prefixes; same catalog/schema by default. |

**Suggested demo order:** deploy/run **`network_analytics_pipeline`** → run the three shard notebooks (or copy real gz files into `cell_towers/`, `kpis/`, `demand/`) → **`databricks bundle run ops_app_network_analytics_pipeline`**. Query **`ops_app_gold_downtown_building_coverage`** for the merged building × coverage × synthetic ops overlay.

Details, expectations, migration (**MV → streaming** for `bronze_cell_towers` and **`ops_app_bronze_*`**), and troubleshooting live in [**network_analytics_pipeline/README.md**](network_analytics_pipeline/README.md) and [**network_analytics_pipeline/detailed_readme.md**](network_analytics_pipeline/detailed_readme.md).

## Genie Code Instruction Pattern

Each notebook follows the same structure:

1. **Markdown cells with instructions** — describe the objective, data sources, expected output, and any constraints (e.g., "filter to T-Mobile only", "use Databricks Spatial SQL")
2. **A refined plan cell** — Genie Code adds this after reading the instructions, summarizing its approach before writing code
3. **Generated code cells** — written, executed, and debugged by Genie Code based on the instructions
4. **Validation cell** — confirms row counts, schemas, and function compatibility

This pattern lets you version-control the _intent_ (markdown) separately from the _implementation_ (code), and regenerate the code by clearing outputs and re-running with Genie Code.

# LoRa Network Analytics on Databricks — Walkthrough

**Goal:** rebuild the Alteryx workflow (cell sites → which Top-100 Small/Rural County Market they sit in) on Databricks, with the day-to-day editing surface as visual and familiar as Alteryx Designer.

## Assets in this folder

| Asset | Type | What it does |
|---|---|---|
| `00_bronze_ingestion.py` | Python notebook | One-time ingest of CSV + MapInfo TAB → bronze Delta tables. Parameterized via widgets. |
| `01_dlt_lakeflow_pipeline.sql` | SQL notebook | The Lakeflow Declarative Pipeline. Open this in Designer for the visual canvas. |
| `README.md` | Markdown | Quick-start guide for setting up the demo. |
| `WALKTHROUGH.md` (this file) | Markdown | Detailed walkthrough — Alteryx↔Designer mapping, editing, scheduling. |

**Delta tables generated** (in your chosen catalog/schema):

| Layer | Tables | Rows (demo data) |
|---|---|---|
| Bronze (notebook) | `sites_bronze`, `county_markets_bronze` | 1,000 / 2,179 |
| Silver / Gold (pipeline) | `sites_silver`, `markets_silver`, `sites_in_market`, `sites_nearest_market`, `site_market_enriched` | 1,000 / 2,179 / 1,000 / 0 / 1,000 |


**Architecture — two layers:**

```
   Volume files                Bronze ingestion              Lakeflow Designer pipeline
 (CSV, MapInfo TAB)   ──▶   (one notebook, runs       ──▶   (visual canvas, runs on a
                              when source refreshes)         schedule; this is the
                                                             Alteryx Designer equivalent)
```

Why split? MapInfo `.TAB` is proprietary and needs `geopandas`. Lakeflow Designer doesn't read it directly, so we handle it once in a notebook. Everything else — formulas, spatial joins, find-nearest, output — lives on the Designer canvas.

---

## Files in this demo

| File | Purpose |
|---|---|
| `00_bronze_ingestion.py` | One-time: reads CSV + MapInfo TAB from the Volume, writes `sites_bronze` and `county_markets_bronze` Delta tables. |
| `01_dlt_lakeflow_pipeline.sql` | The Lakeflow Declarative Pipeline. **This is the file Designer opens as the visual canvas.** |

Once you create the Lakeflow pipeline pointing at `01_dlt_lakeflow_pipeline.sql`, it will appear in **Workflows → Pipelines** under whatever name you give it (e.g., `lora_network_analytics`).

---

## Step 1 — Run bronze ingestion (one-time, or whenever source files change)

1. Import `00_bronze_ingestion.py` into your Databricks workspace.
2. Open it and set the three widgets at the top:
   - `catalog` — your Unity Catalog
   - `schema` — the schema where bronze tables will land
   - `volume_path` — Volume path holding `site_lat_long.csv` and `Top100_SmallRural_County_Markets_2021.TAB`
3. Attach to **Serverless** (top right).
4. Run All.
5. Verify the two output tables exist:
   ```sql
   SELECT COUNT(*) FROM <catalog>.<schema>.sites_bronze;          -- 1,000
   SELECT COUNT(*) FROM <catalog>.<schema>.county_markets_bronze; -- 2,179
   ```

Expected runtime: ~2 min. After this, the bronze layer is ready for any number of downstream pipelines.

---

## Step 2 — Create the Lakeflow pipeline and open in Designer

1. Import `01_dlt_lakeflow_pipeline.sql` into your workspace.
2. **Workflows → Pipelines → Create pipeline**.
3. Pick **Serverless**, choose a destination catalog + schema for the pipeline outputs (where silver/gold will land).
4. Under **Source code → Notebook libraries**, point at `01_dlt_lakeflow_pipeline.sql`.
5. Under **Advanced → Configuration**, add two keys (these tell the pipeline where to read the bronze tables you produced in step 1):
   - `bronze_catalog` → same value you used in the bronze widget
   - `bronze_schema`  → same value you used in the bronze widget
6. Save.
7. Click **"Open in Designer"** (top right of the pipeline detail page).
8. You'll see the visual canvas — five boxes (one per materialized view), connected by arrows showing data flow. This is the equivalent of the Alteryx canvas.

### What's on the canvas — Alteryx mapping

| Designer node | Alteryx tool | What it does |
|---|---|---|
| `sites_silver` | **Formula tool** (`IIF(isnull(...))`) | `COALESCE` on lat/long; passes through to next nodes |
| `markets_silver` | **Input Data (MapInfo TAB)** | Reads the polygons that the bronze notebook stages |
| `sites_in_market` | **Spatial Match → "Target within Universe"** | Point-in-polygon join: `ST_Contains(polygon, ST_Point(lon,lat))` |
| `sites_nearest_market` | **Find Nearest** | `ST_DistanceSphere` + `ROW_NUMBER()` over centroid distances; fires only for sites with no containing polygon |
| `site_market_enriched` | **Browse / Output Data** | Gold table that unions WITHIN-matches and NEAREST-matches with a `match_type` tag |

Click any node to see its SQL on the right pane — that's the equivalent of double-clicking an Alteryx tool to edit its config.

---

## Step 3 — Editing the pipeline visually

Designer supports these in the right-hand pane for any node:

- **Edit SQL** — paste/edit the body. Save → Designer re-validates the canvas and updates downstream arrows automatically.
- **Add a new node** — `+` button on an arrow. Pick a transformation type (filter, aggregate, join, custom SQL) and Designer wires it in.
- **Preview data** — click a node, hit "Sample" to see 100 rows mid-pipeline. Same as the Alteryx data viewer.
- **Lineage** — Designer shows column-level lineage automatically.

For anything spatial that doesn't have a built-in node, use **Custom SQL transformation** — that's where the `ST_*` functions live.

---

## Step 4 — Run the pipeline

From Designer **or** from the pipeline page:

- **"Start"** (top right) → runs once.
- **"Schedule"** → cron / continuous. Pick "Triggered" for ad-hoc, "Continuous" for streaming refresh.
- **"Settings → Production mode"** → flips it from dev (cheap, single small cluster) to prod (parallelism + retries).

Runtime on serverless: ~2 minutes end-to-end for this dataset.

---

## Step 5 — Operationalize: chain bronze + pipeline together

Production setup once it's working:

1. Wrap `00_bronze_ingestion` as a **Job task #1**.
2. Add a **Pipeline task #2** that runs `lora_network_analytics`. Mark it `depends_on: bronze_ingestion`.
3. Schedule the job (e.g., daily at 6am).

That's the full Alteryx-workflow-with-Crew-or-Server equivalent: one orchestrated unit that runs source → bronze → silver → gold every day.

---

## Output: `site_market_enriched`

Every site has exactly one row, with:

| Column | Notes |
|---|---|
| `site_id` | from sites CSV |
| `S_SITE_LATITUDE`, `S_SITE_LONGITUDE` | post-COALESCE coordinates |
| `market_id`, `market_name`, `state` | the market this site maps to |
| `market_type_detailed`, `population` | from the MapInfo attributes |
| `match_type` | `'WITHIN'` (inside polygon) or `'NEAREST'` (closest centroid) |
| `distance_km` | `0.0` for WITHIN matches, real distance for NEAREST |

Query examples:

```sql
-- Site coverage by market
SELECT market_name, state, COUNT(*) AS sites
FROM <catalog>.<schema>.site_market_enriched
GROUP BY market_name, state
ORDER BY sites DESC;

-- Sites that fell outside every market (and how far to the nearest one)
SELECT site_id, market_name, distance_km
FROM <catalog>.<schema>.site_market_enriched
WHERE match_type = 'NEAREST'
ORDER BY distance_km DESC;
```

---

## FAQ / gotchas

**"My polygons aren't matching anything."**
Check the CRS in the bronze step. Source MapInfo files are usually in a US state-plane or NAD83 projection; we reproject to WGS84 (EPSG:4326) to match the lat/long CSV. If you skip the reproject, `ST_Contains` returns nothing.

**"Why is geometry stored as WKT strings, not GEOMETRY?"**
Portability across DBR versions. The native `GEOMETRY` Delta type needs the `delta.feature.geospatial` table feature, which older runtimes (16.4 LTS and below) reject. WKT works everywhere. On DBR 17+ you can switch to native GEOMETRY by changing the silver tables to wrap geometry in `ST_GeomFromWKT(...)`.

**"Can I drop geopandas and ingest TAB directly in Designer?"**
Not today. Designer's source connectors don't include MapInfo. The bronze notebook is the seam — keep it small, run it on-demand, leave Designer for what it does best (visual joins, formulas, aggregates).

**"How do I add Find Nearest distance threshold (e.g., 'reject if > 50 km')?"**
Add a Filter node on the `sites_nearest_market` output: `WHERE distance_km < 50`. Designer's UI has a built-in filter node, no SQL needed.

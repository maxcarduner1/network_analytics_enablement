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

## Step 2 — Build the Lakeflow pipeline

Two paths. Pick **Path A** if you want a working pipeline in 5 minutes, or **Path B** if you want to feel the Alteryx-style drag-and-drop experience (recommended for the customer demo — this is where the "Designer ≈ Alteryx Designer" story lands).

---

### Path A — import the prebuilt SQL (fast)

1. Import `01_dlt_lakeflow_pipeline.sql` into your workspace.
2. **Workflows → Pipelines → Create pipeline**.
3. Pick **Serverless**, choose a destination catalog + schema for pipeline outputs.
4. Under **Source code → Notebook libraries**, point at `01_dlt_lakeflow_pipeline.sql`.
5. Under **Advanced → Configuration**, add:
   - `bronze_catalog` → same value you used in the bronze widget
   - `bronze_schema`  → same value you used in the bronze widget
6. **Save** → **Start**.
7. Click **"Open in Designer"** to see the 5-node canvas.

---

### Path B — build it node-by-node in Designer UI (Alteryx-feel demo)

This is the demo path. The customer rebuilds her Alteryx canvas one box at a time. ~15 min walkthrough.

**Create an empty pipeline first:**

1. **Workflows → Pipelines → Create pipeline → Empty pipeline**.
2. Pick **Serverless**, destination catalog + schema for the pipeline outputs.
3. Save with name `lora_network_analytics` (or whatever).
4. Click **"Open in Designer"**. You'll land on an empty canvas.

**Now add the 5 nodes — each maps 1:1 to one Alteryx tool:**

#### Node 1 — `sites_silver`  (≈ Alteryx Formula tool)

1. On the canvas, click **+ Add → From dataset / Source**.
2. Browse to `<bronze_catalog>.<bronze_schema>.sites_bronze` → select.
3. With the new node selected, click **+ Add downstream → Custom SQL transformation** (or **Project / Formula** node if you prefer the visual builder).
4. Name the output **`sites_silver`**.
5. Paste this SQL (it's the COALESCE formula from the Alteryx canvas):
   ```sql
   SELECT
     site_id,
     CAST(SITE_LATITUDE  AS DOUBLE) AS SITE_LATITUDE,
     CAST(SITE_LONGITUDE AS DOUBLE) AS SITE_LONGITUDE,
     CAST(SITE_LATITUDE  AS DOUBLE) AS S_SITE_LATITUDE,
     CAST(SITE_LONGITUDE AS DOUBLE) AS S_SITE_LONGITUDE
   FROM sites_bronze
   WHERE SITE_LATITUDE IS NOT NULL AND SITE_LONGITUDE IS NOT NULL
   ```
6. Click **Sample** in the right pane to see ~100 rows of output — same as the Alteryx data viewer.

#### Node 2 — `markets_silver`  (≈ Alteryx Input Data MapInfo TAB)

1. **+ Add → From dataset / Source** on the canvas.
2. Browse to `<bronze_catalog>.<bronze_schema>.county_markets_bronze` → select.
3. **+ Add downstream → Custom SQL transformation**, output name **`markets_silver`**.
4. Paste:
   ```sql
   SELECT
     ID                  AS market_id,
     Name                AS market_name,
     State               AS state,
     MarketType          AS market_type,
     MarketTypeDetailed  AS market_type_detailed,
     Population          AS population,
     geometry_wkt
   FROM county_markets_bronze
   WHERE geometry_wkt IS NOT NULL
   ```

You now have two parallel branches on the canvas — sites on one side, markets on the other. Exactly like the Alteryx layout.

#### Node 3 — `sites_in_market`  (≈ Alteryx Spatial Match → "Target within Universe")

This is the point-in-polygon join — the centerpiece of the workflow.

1. With `sites_silver` selected, click **+ Add downstream → Join** (or **Custom SQL** if you want the join condition explicit).
2. Set the other input to **`markets_silver`**.
3. For **Join type**, pick **Inner join**.
4. For the **Join condition**, switch to **Custom condition** and paste:
   ```sql
   ST_Contains(
     ST_GeomFromWKT(markets_silver.geometry_wkt),
     ST_Point(sites_silver.S_SITE_LONGITUDE, sites_silver.S_SITE_LATITUDE)
   )
   ```
5. Name the output **`sites_in_market`**. Choose the columns you want to keep:
   - From `sites_silver`: `site_id`, `S_SITE_LATITUDE`, `S_SITE_LONGITUDE`
   - From `markets_silver`: `market_id`, `market_name`, `state`, `market_type_detailed`, `population`
6. **Sample** — you should see ~1,000 rows for the demo dataset (one row per site that's inside a polygon).

#### Node 4 — `sites_nearest_market`  (≈ Alteryx Find Nearest)

This one needs a custom SQL block because window functions over `ST_DistanceSphere` aren't expressible with a single visual node.

1. **+ Add → Custom SQL transformation** anywhere on the canvas (it can read from any other node — Designer will draw the arrows for you).
2. Output name **`sites_nearest_market`**.
3. Paste:
   ```sql
   WITH unmatched AS (
     SELECT s.*
     FROM sites_silver s
     LEFT ANTI JOIN sites_in_market m USING (site_id)
   ),
   ranked AS (
     SELECT
       u.site_id,
       u.S_SITE_LATITUDE,
       u.S_SITE_LONGITUDE,
       m.market_id,
       m.market_name,
       m.state,
       m.market_type_detailed,
       m.population,
       ST_DistanceSphere(
         ST_Point(u.S_SITE_LONGITUDE, u.S_SITE_LATITUDE),
         ST_Centroid(ST_GeomFromWKT(m.geometry_wkt))
       ) AS distance_m,
       ROW_NUMBER() OVER (
         PARTITION BY u.site_id
         ORDER BY ST_DistanceSphere(
           ST_Point(u.S_SITE_LONGITUDE, u.S_SITE_LATITUDE),
           ST_Centroid(ST_GeomFromWKT(m.geometry_wkt))
         )
       ) AS rn
     FROM unmatched u
     CROSS JOIN markets_silver m
   )
   SELECT site_id, S_SITE_LATITUDE, S_SITE_LONGITUDE,
          market_id, market_name, state, market_type_detailed, population,
          ROUND(distance_m / 1000.0, 2) AS distance_km
   FROM ranked
   WHERE rn = 1
   ```
4. **Sample** — for the demo data this is empty (all sites fell inside a market). That's expected; the node exists to handle real-world inputs where some sites sit outside the Top-100 polygons.

#### Node 5 — `site_market_enriched`  (≈ Alteryx Browse / final Output Data)

1. **+ Add → Union** on the canvas (or **Custom SQL** with `UNION ALL`).
2. Inputs: **`sites_in_market`** and **`sites_nearest_market`**.
3. Add two computed columns so downstream consumers know how each row got matched:
   - On the `sites_in_market` branch: `'WITHIN'` AS `match_type`, `0.0` AS `distance_km`
   - On the `sites_nearest_market` branch: `'NEAREST'` AS `match_type`, `distance_km` (already exists)
4. Output name **`site_market_enriched`**.

If you prefer to write the union explicitly as Custom SQL:
```sql
SELECT site_id, S_SITE_LATITUDE, S_SITE_LONGITUDE,
       market_id, market_name, state, market_type_detailed, population,
       'WITHIN'  AS match_type,
       0.0       AS distance_km
FROM sites_in_market
UNION ALL
SELECT site_id, S_SITE_LATITUDE, S_SITE_LONGITUDE,
       market_id, market_name, state, market_type_detailed, population,
       'NEAREST' AS match_type,
       distance_km
FROM sites_nearest_market
```

**Validate and run:**

1. Click **Validate** (top of canvas) — Designer checks the SQL of every node and shows any errors inline.
2. Click **Start** to run the pipeline once. Watch each node turn green as it materializes.
3. Open the **Lineage** tab to see the column-level lineage Designer built automatically.

You now have a working 5-node pipeline you built entirely in the UI — same logic as her Alteryx workflow, same canvas-style editing.

### Alteryx ↔ Designer mapping reference

| Designer node | Alteryx tool | What it does |
|---|---|---|
| `sites_silver` | **Formula tool** (`IIF(isnull(...))`) | `COALESCE` on lat/long; passes through to next nodes |
| `markets_silver` | **Input Data (MapInfo TAB)** | Reads the polygons that the bronze notebook stages |
| `sites_in_market` | **Spatial Match → "Target within Universe"** | Point-in-polygon join: `ST_Contains(polygon, ST_Point(lon,lat))` |
| `sites_nearest_market` | **Find Nearest** | `ST_DistanceSphere` + `ROW_NUMBER()` over centroid distances; fires only for sites with no containing polygon |
| `site_market_enriched` | **Browse / Output Data** | Gold table that unions WITHIN-matches and NEAREST-matches with a `match_type` tag |

Click any node to see its SQL on the right pane — equivalent of double-clicking an Alteryx tool to edit its config.

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

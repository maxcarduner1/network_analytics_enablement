# Network Analytics — End-to-End Demo Guide

A walkthrough of the full stack: raw files in a Volume → Bronze/Silver/Gold via a serverless **Lakeflow Spark Declarative Pipeline** (**SDP**) → three ways to query the result (Genie, Knowledge Assistant, Supervisor Agent). See [SDP documentation](https://docs.databricks.com/aws/en/ldp).

Workspace: [`fevm-cmegdemos`](https://fevm-cmegdemos.cloud.databricks.com)

---

## 1. Architecture

```
        ┌────────────────────────────────────────────────────────┐
        │  RAW (UC Volume: raw_data)                             │
        │  bdc_53_5GNR_..._14apr2026.zip │ Washington.zip │ 310.csv.gz
        └─────────────────────┬──────────────────────────────────┘
                              │  serverless SDP pipeline
        ┌─────────────────────▼──────────────────────────────────┐
        │  BRONZE — raw, deduplicated, schema-typed              │
        │  bronze_fcc_bdc_h3   bronze_building_footprints        │
        │  bronze_cell_towers                                    │
        └─────────────────────┬──────────────────────────────────┘
                              │  expectations + Seattle bbox + carrier filter
        ┌─────────────────────▼──────────────────────────────────┐
        │  SILVER — Seattle metro, native GEOMETRY, T-Mobile only│
        │  silver_fcc_bdc_h3_seattle      silver_building_footprints_seattle
        │  silver_tmobile_towers_seattle                         │
        └─────────────────────┬──────────────────────────────────┘
                              │  H3 join + nearest-tower haversine
        ┌─────────────────────▼──────────────────────────────────┐
        │  GOLD — analysis-ready                                 │
        │  gold_downtown_building_coverage                       │
        │  gold_coverage_by_distance_bucket                      │
        └─────────────────────┬──────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────────────────┐
        ▼                     ▼                                 ▼
   ┌─────────┐          ┌─────────────┐                ┌──────────────┐
   │  Genie  │          │  KA (RAG)   │  ◄── PDF ──┐   │  KA doc PDF  │
   │  space  │          │  endpoint   │             │   │  (UC Volume) │
   │ (SQL)   │          │             │             └───┤  ka_doc/     │
   └────┬────┘          └──────┬──────┘                 └──────────────┘
        │                      │
        └──────┬───────────────┘
               ▼
   ┌──────────────────────────────────────┐
   │  Supervisor Agents (route + compose) │
   │  • AB MAS  : mas-25cf5601-endpoint   │  ← Agent Bricks Multi-Agent (UI)
   │  • Code SA : network-analytics-      │  ← LangGraph (custom code)
   │              supervisor              │
   └──────────────────┬───────────────────┘
                      ▼
                  End User
```

---

## 2. Catalog & Volumes

| Resource | Path |
| --- | --- |
| Catalog | [`cmegdemos_catalog`](https://fevm-cmegdemos.cloud.databricks.com/explore/data/cmegdemos_catalog) |
| Schema | [`cmegdemos_catalog.network_analytics_enablement`](https://fevm-cmegdemos.cloud.databricks.com/explore/data/cmegdemos_catalog/network_analytics_enablement) |
| Raw files volume | [`raw_data`](https://fevm-cmegdemos.cloud.databricks.com/explore/data/volumes/cmegdemos_catalog/network_analytics_enablement/raw_data) |
| KA source PDF volume | [`ka_doc`](https://fevm-cmegdemos.cloud.databricks.com/explore/data/volumes/cmegdemos_catalog/network_analytics_enablement/ka_doc) |

---

## 3. Lakeflow SDP pipeline

[**`network_analytics_pipeline`** (open in Pipelines UI)](https://fevm-cmegdemos.cloud.databricks.com/pipelines/ce588a95-7d89-4e58-a907-71364c01390f)

- **Pipeline ID:** `ce588a95-7d89-4e58-a907-71364c01390f`
- **Mode:** serverless
- **Source:** Databricks Asset Bundle at `network_analytics_pipeline/` ([databricks.yml](network_analytics_pipeline/databricks.yml))

**Expectation severity legend** — `@dp.expect` (warn-only, row kept), `@dp.expect_or_drop` (violating rows dropped), `@dp.expect_or_fail` (whole update aborts).

### 3.1 Bronze — raw schema-typed

#### [`bronze_fcc_bdc_h3`](https://fevm-cmegdemos.cloud.databricks.com/explore/data/cmegdemos_catalog/network_analytics_enablement/bronze_fcc_bdc_h3)
Source: `bdc_53_5GNR_mobile_broadband_h3_J25_14apr2026.zip` (GeoPackage) — [bronze_fcc_bdc_h3.py](network_analytics_pipeline/src/bronze/bronze_fcc_bdc_h3.py)

```python
@dp.expect("valid_fid", "fid IS NOT NULL")
```
The FCC's internal feature ID should always be present. Warn-only because a missing fid is suspicious but doesn't break joins (we join on `h3_res9_id`).

```python
@dp.expect_or_drop("parsable_h3", "h3_isvalid(h3_res9_id)")
```
The H3 ID is the join key for everything downstream. If `h3_isvalid` returns false the row is unusable — drop it so silver/gold don't carry corrupt hex IDs.

```python
@dp.expect("known_technology", "technology IS NOT NULL")
```
Every BDC row should declare a technology code (3G/4G/5G). Warn-only — we filter by code in silver, so a NULL would just be excluded there anyway.

#### [`bronze_building_footprints`](https://fevm-cmegdemos.cloud.databricks.com/explore/data/cmegdemos_catalog/network_analytics_enablement/bronze_building_footprints)
Source: `Washington.zip` (ESRI Shapefile) — [bronze_building_footprints.py](network_analytics_pipeline/src/bronze/bronze_building_footprints.py)

```python
@dp.expect_or_drop("wkt_present", "wkt IS NOT NULL")
```
The whole point of this table is the geometry. A row without WKT has nothing to contribute — drop.

```python
@dp.expect_or_drop("polygon_format", "wkt LIKE 'POLYGON%'")
```
Microsoft footprints should always be polygons. If a row comes in as a LINESTRING or POINT, the shapefile got mangled — drop rather than risk a downstream `ST_Centroid` on a non-polygon.

```python
@dp.expect("non_negative_height", "height IS NULL OR height >= 0")
```
Negative building height is nonsensical but rare and might be a sentinel for "unknown." Warn-only so the metric surfaces if a bad batch lands.

#### [`bronze_cell_towers`](https://fevm-cmegdemos.cloud.databricks.com/explore/data/cmegdemos_catalog/network_analytics_enablement/bronze_cell_towers)
**Source:** headerless gzip CSV shards under **`raw_data/cell_towers/*.csv.gz`** — [**Auto Loader**](https://docs.databricks.com/aws/en/ingestion/auto-loader/index) (`cloudFiles`) into a **streaming** Delta table — [bronze_cell_towers.py](network_analytics_pipeline/src/bronze/bronze_cell_towers.py). Append-only: each pipeline update ingests **new** files since the last checkpoint.

**Canonical extract:** keep **`310.csv.gz`** at the volume root if you still run **`01_Ingest.ipynb`** or use [**demo_generate_cell_towers_shard.ipynb**](network_analytics_pipeline/notebooks/demo_generate_cell_towers_shard.ipynb) to sample random shards **into `cell_towers/`** for incremental demos.

**Migration:** If you previously deployed `bronze_cell_towers` as a materialized view, SDP cannot flip it to streaming in place — run **`DROP TABLE`** then redeploy (see [scripts/drop_bronze_cell_towers_for_streaming_migration.sql](network_analytics_pipeline/scripts/drop_bronze_cell_towers_for_streaming_migration.sql)).

```python
@dp.expect_or_drop("non_null_cell", "cell IS NOT NULL")
```
Cell ID is the primary identifier for a tower's sector. No cell ID = the row can't be deduplicated or joined — drop.

```python
@dp.expect_or_drop("valid_mcc_310", "mcc = 310")
```
The CSV is supposed to be US-only (Mobile Country Code 310). If an MCC=302 (Canada) row leaks in, drop it before downstream silver assumes US bbox checks make sense.

```python
@dp.expect_or_drop("valid_lat_lon", "lat BETWEEN -90 AND 90 AND lon BETWEEN -180 AND 180")
```
Out-of-range coordinates would crash later spatial functions. Drop them at the source.

### 3.2 Silver — Seattle bbox, GEOMETRY(4326), carrier filter

#### [`silver_fcc_bdc_h3_seattle`](https://fevm-cmegdemos.cloud.databricks.com/explore/data/cmegdemos_catalog/network_analytics_enablement/silver_fcc_bdc_h3_seattle)
[silver_fcc_bdc_h3_seattle.py](network_analytics_pipeline/src/silver/silver_fcc_bdc_h3_seattle.py)

```python
@dp.expect_or_drop("in_seattle_bbox",
    "center_lat BETWEEN ... AND center_lon BETWEEN ...")
```
Bronze covers all of Washington state. Silver is the Seattle metro slice. Hexes whose center falls outside the bbox are dropped — they'd just be noise for the building-coverage analysis.

```python
@dp.expect("known_5g_technology", "technology IS NOT NULL")
```
Mirror of the bronze rule, kept as a warning here so the silver-layer metrics flag any rows that slipped through with NULL technology.

```python
@dp.expect("non_null_speeds", "mindown IS NOT NULL AND minup IS NOT NULL")
```
A row with NULL speeds is technically valid BDC but useless for coverage analysis. Warn so we can see the drop rate over time without aggressively removing rows.

#### [`silver_building_footprints_seattle`](https://fevm-cmegdemos.cloud.databricks.com/explore/data/cmegdemos_catalog/network_analytics_enablement/silver_building_footprints_seattle)
[silver_building_footprints_seattle.py](network_analytics_pipeline/src/silver/silver_building_footprints_seattle.py)

```python
@dp.expect_or_drop("valid_geometry", "ST_IsValid(geometry)")
```
Bronze stored WKT strings; silver promotes them to native `GEOMETRY(4326)`. If the parsed geometry is self-intersecting or otherwise invalid, drop — every downstream `ST_*` call would error.

```python
@dp.expect_or_drop("centroid_in_seattle_bbox", "ST_Y(ST_Centroid(geometry)) BETWEEN ...")
```
Filter the WA-state buildings down to Seattle metro. Centroid-based: a building straddling the bbox edge is included if its center is inside.

```python
@dp.expect("non_negative_height", "height IS NULL OR height >= 0")
```
Same warn-only check as bronze — repeated here so silver metrics show the violation rate even after dropping out-of-bbox rows.

#### [`silver_tmobile_towers_seattle`](https://fevm-cmegdemos.cloud.databricks.com/explore/data/cmegdemos_catalog/network_analytics_enablement/silver_tmobile_towers_seattle)
[silver_tmobile_towers_seattle.py](network_analytics_pipeline/src/silver/silver_tmobile_towers_seattle.py)

```python
@dp.expect_or_fail("tmobile_only", f"net = {TMOBILE_MNC}")
```
If a non-T-Mobile tower somehow shows up here, the entire update fails. That's correct — you'd rather stop and figure out why a Verizon tower got in than silently produce a corrupted "T-Mobile coverage" gold table.

```python
@dp.expect_or_fail("point_geometry", "ST_GeometryType(location) = 'ST_Point'")
```
Towers must be points, not polygons or linestrings. The downstream haversine logic assumes a single (lat, lon) pair — fail loudly if the schema breaks.

```python
@dp.expect_or_drop("in_seattle_bbox", "latitude BETWEEN ... AND longitude BETWEEN ...")
```
Bronze is US-wide. Drop towers outside Seattle metro so the cross-join in gold stays cheap.

```python
@dp.expect("radius_positive", "coverage_radius_m IS NULL OR coverage_radius_m > 0")
```
A zero or negative radius is suspicious but tolerable since we don't currently use radius for matching. Warn so we'd notice if it becomes systematic.

### 3.3 Gold — analysis-ready

#### [`gold_downtown_building_coverage`](https://fevm-cmegdemos.cloud.databricks.com/explore/data/cmegdemos_catalog/network_analytics_enablement/gold_downtown_building_coverage)
[gold_downtown_building_coverage.py](network_analytics_pipeline/src/gold/gold_downtown_building_coverage.py)

```python
@dp.expect_or_fail("non_negative_speeds",
    "best_download_mbps >= 0 AND best_upload_mbps >= 0")
```
Negative throughput is impossible. If silver leaks one through, abort — this would be visible in customer-facing dashboards within minutes.

```python
@dp.expect_or_fail("reasonable_distance", "distance_to_tower_m BETWEEN 0 AND 50000")
```
The "nearest" tower should be within 50 km. A 5,000 km value means the haversine math broke (lat/lon swapped, projection mismatch). Fail rather than ship an obviously wrong column.

```python
@dp.expect_or_fail("valid_h3", "h3_isvalid(h3_res9_id)")
```
Same H3 sanity check as bronze, but here as a hard fail — the gold table is what dashboards and the Genie sit on top of, so a bad hex would silently corrupt every map.

```python
@dp.expect_or_drop("has_nearest_tower", "nearest_tower_id IS NOT NULL")
```
Buildings with no nearest-tower match (shouldn't happen with a CROSS JOIN, but defends against an empty `silver_tmobile_towers_seattle` upstream) are dropped — keeping them would muddle the distance-to-tower analysis.

```python
@dp.expect("has_5g_coverage", "best_download_mbps > 0")
```
Many downtown buildings have zero reported 5G coverage — that's actually a finding, not an error. Warn-only, so the metric tells us *what fraction* of buildings have no coverage filed.

#### [`gold_coverage_by_distance_bucket`](https://fevm-cmegdemos.cloud.databricks.com/explore/data/cmegdemos_catalog/network_analytics_enablement/gold_coverage_by_distance_bucket)
[gold_coverage_by_distance_bucket.py](network_analytics_pipeline/src/gold/gold_coverage_by_distance_bucket.py)

```python
@dp.expect_or_fail("bucket_has_buildings", "buildings > 0")
```
Each distance-bucket row aggregates over multiple buildings; a row with `buildings = 0` would mean a GROUP BY produced an empty group, which shouldn't be possible. Fail to surface the bug.

```python
@dp.expect_or_fail("non_negative_avg_speed", "avg_download_mbps >= 0")
```
Average of non-negative numbers can't be negative. If this fires, the underlying gold table had bad data — fail.

```python
@dp.expect("non_null_avg_speed", "avg_download_mbps IS NOT NULL")
```
A NULL average usually means an empty group survived the prior `expect_or_fail`. Warn-only as a defense in depth.

### 3.4 Reference notebooks (alternative imperative path)

The same pipeline can be expressed as plain notebooks (bypassing the managed SDP pipeline):

- [`01_Ingest.ipynb`](01_Ingest.ipynb)
- [`02_Analysis.ipynb`](02_Analysis.ipynb)
- [`00_instructions.ipynb`](00_instructions.ipynb) — Genie Code instruction pattern

---

## 4. AI Layer

| Component | Endpoint | Experiment | UI |
| --- | --- | --- | --- |
| **Genie space** | — | — | [open](https://fevm-cmegdemos.cloud.databricks.com/genie/rooms/01f1434885a51fc4bf4d5fbf5d3fb928) |
| **Knowledge Assistant** *(BDC methodology)* | [`ka-62de30a2-endpoint`](https://fevm-cmegdemos.cloud.databricks.com/ml/endpoints/ka-62de30a2-endpoint) | [`4298816059357076`](https://fevm-cmegdemos.cloud.databricks.com/ml/experiments/4298816059357076/traces) | [open KA](https://fevm-cmegdemos.cloud.databricks.com/agents/knowledge-assistants/62de30a2-26cd-4992-903b-5610864ef504) |
| **AB Multi-Agent Supervisor** | [`mas-25cf5601-endpoint`](https://fevm-cmegdemos.cloud.databricks.com/ml/endpoints/mas-25cf5601-endpoint) | [`4298816059358721`](https://fevm-cmegdemos.cloud.databricks.com/ml/experiments/4298816059358721/traces) | [open Agent Bricks](https://fevm-cmegdemos.cloud.databricks.com/agent-bricks) |
| **Code-based Supervisor** *(LangGraph)* | [`network-analytics-supervisor`](https://fevm-cmegdemos.cloud.databricks.com/ml/endpoints/network-analytics-supervisor) | [`4298816059358720`](https://fevm-cmegdemos.cloud.databricks.com/ml/experiments/4298816059358720/traces) | [03_supervisor_agent.py](03_supervisor_agent.py) |
| KA source document | `/Volumes/cmegdemos_catalog/network_analytics_enablement/ka_doc/fcc_bdc_methodology.pdf` | — | [open volume](https://fevm-cmegdemos.cloud.databricks.com/explore/data/volumes/cmegdemos_catalog/network_analytics_enablement/ka_doc) |

---

## 5. Sample questions & expected behavior

### 5.1 Ask the **Genie** *(structured queries)*

| # | Question | Expected behavior |
| --- | --- | --- |
| G1 | *How many downtown Seattle buildings have any 5G coverage?* | SELECT COUNT(*) FROM `gold_downtown_building_coverage` WHERE `best_download_mbps > 0` → ~1,000 |
| G2 | *What's the average download speed by distance bucket?* | Reads `gold_coverage_by_distance_bucket`; returns table sorted by bucket |
| G3 | *Which 10 buildings are closest to a T-Mobile tower?* | ORDER BY `distance_to_tower_m` ASC LIMIT 10 |
| G4 | *Show me an H3 cell with the most provider records.* | GROUP BY `h3_res9_id`, COUNT(DISTINCT provider) → top hex |
| G5 | *Plot best_download_mbps vs distance_to_tower_m.* | Returns scatter chart (Genie auto-suggests viz) |

### 5.2 Ask the **Knowledge Assistant** *(methodology / interpretation)*

| # | Question | Expected behavior |
| --- | --- | --- |
| K1 | *What does `mindown` represent — measured or advertised?* | Cites §3, says "advertised floor, provider self-reported, not field-tested" |
| K2 | *What does technology code 82 mean?* | "5G-NR" with reference to mobile code table §4 |
| K3 | *How does the BDC challenge process work?* | Summarizes §6 — availability vs. bulk challenges, applied next filing |
| K4 | *Why might a building near a tower still show weak coverage?* | Lists outdoor-only modeling, building penetration, backhaul, mmWave LoS — §5 + §7 |
| K5 | *When was this dataset filed?* | "June 2025 (J25), released April 2026" — §2 |

### 5.3 Ask a **Supervisor Agent** *(routes to Genie + KA, optionally synthesizes)*

Use either `mas-25cf5601-endpoint` (Agent Bricks) or `network-analytics-supervisor` (code) — they accept the same shape of questions and should produce comparable answers.

| # | Question | Expected routing |
| --- | --- | --- |
| S1 | *What's the average best_download_mbps in downtown Seattle and how should I interpret that number?* | Genie → numeric mean; KA → "advertised, not measured" caveat; supervisor synthesizes |
| S2 | *Show me a building with mindown=0 next to a tower, and explain why that's possible.* | Genie returns one row; KA explains: tower could be small-cell/backhaul, or provider didn't file the hex |
| S3 | *Which distance bucket has the worst coverage and what could explain it?* | Genie → bucket aggregation; KA → free-space path-loss + building-penetration discussion |
| S4 | *Is `low_latency = true` reliable in our data?* | KA-only; supervisor recognizes pure-methodology question and skips Genie |
| S5 | *How many H3 cells have `mindown >= 100`?* | Genie-only; supervisor skips KA |

### 5.4 Ask a **specialist Solution Architect (SA) flow** *(human-in-the-loop demo)*

These show how the SA uses the assets to drive a customer conversation:

| # | Customer prompt to SA | SA does |
| --- | --- | --- |
| SA1 | *"Can your platform tell me where my buildings have weak 5G?"* | Open Genie → run G1; open KA → ask K4 to set expectations on what "weak" means |
| SA2 | *"How do I trust the data?"* | KA → K1, K3 (governance + challenge process). Then point at the SDP pipeline **Data Quality** / expectations metrics in §3 |
| SA3 | *"Can I plug in my own carrier?"* | Show `silver_tmobile_towers_seattle.py` filter — change MNC. Discuss bronze→silver pattern |
| SA4 | *"Who is using this assistant?"* | Open the supervisor's MLflow experiment, filter traces by `end_user_email` tag |
| SA5 | *"Build me a custom assistant on my own docs."* | Show `Volume → Knowledge Assistant create-knowledge-source` flow we used here |

---

## 6. Trace observability — `end_user_email` tag

Every call routed through the code-based supervisor (`network-analytics-supervisor`) sets an `end_user_email` tag on the MLflow trace, resolved from `custom_inputs.end_user_email`.

For the AB MAS and KA endpoints (managed services), pass user identity via the `metadata` field in the request body — the trace captures it in `mlflow.traceInputs.metadata`, and a small post-call PATCH can lift it to a first-class tag (see §5 example below).

```bash
# Tag a trace with the calling user (one-liner)
TOKEN=$(databricks auth token --profile fevm-cmegdemos | jq -r .access_token)
curl -s -X PATCH \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  "https://fevm-cmegdemos.cloud.databricks.com/api/3.0/mlflow/traces/<trace_id>/tags" \
  -d '{"key":"end_user_email","value":"razi.bayati@databricks.com"}'
```

Filter MLflow trace search by `tags.end_user_email = "razi.bayati@databricks.com"` to see all traces from a given user.

---

## 7. Demo flow (suggested 10-minute walkthrough)

1. **Open the [Pipeline](https://fevm-cmegdemos.cloud.databricks.com/pipelines/ce588a95-7d89-4e58-a907-71364c01390f)** — show DAG, expectations panel, lineage from raw volume to gold tables. (~2 min)
2. **Open [Catalog Explorer](https://fevm-cmegdemos.cloud.databricks.com/explore/data/cmegdemos_catalog/network_analytics_enablement)** — point out bronze/silver/gold; click a gold table → Sample data + AI Generated description. (~1 min)
3. **Open the [Genie space](https://fevm-cmegdemos.cloud.databricks.com/genie/rooms/01f1434885a51fc4bf4d5fbf5d3fb928)** — ask G1 and G2 from §5.1. (~2 min)
4. **Open the [KA](https://fevm-cmegdemos.cloud.databricks.com/agents/knowledge-assistants/62de30a2-26cd-4992-903b-5610864ef504)** — ask K1 and K4. Show citations back to the [PDF](https://fevm-cmegdemos.cloud.databricks.com/explore/data/volumes/cmegdemos_catalog/network_analytics_enablement/ka_doc). (~2 min)
5. **Open the [AB MAS](https://fevm-cmegdemos.cloud.databricks.com/agent-bricks)** — ask S1 (mixed). Show how the supervisor calls both child agents and synthesizes. (~2 min)
6. **Open the [supervisor's MLflow experiment](https://fevm-cmegdemos.cloud.databricks.com/ml/experiments/4298816059358721/traces)** — filter by `tags.end_user_email`, click a trace, walk through spans (KA call · Genie call · final synthesis). (~1 min)

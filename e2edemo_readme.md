# Network Analytics — End-to-End Demo Guide

A walkthrough of the full stack: raw files in a Volume → Bronze/Silver/Gold via a serverless DLT pipeline → three ways to query the result (Genie, Knowledge Assistant, Supervisor Agent).

Workspace: [`fevm-cmegdemos`](https://fevm-cmegdemos.cloud.databricks.com)

---

## 1. Architecture

```
        ┌────────────────────────────────────────────────────────┐
        │  RAW (UC Volume: raw_data)                             │
        │  bdc_53_5GNR_..._14apr2026.zip │ Washington.zip │ 310.csv.gz
        └─────────────────────┬──────────────────────────────────┘
                              │  serverless DLT pipeline
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

## 3. DLT Pipeline

[**`network_analytics_pipeline`** (open in Pipelines UI)](https://fevm-cmegdemos.cloud.databricks.com/pipelines/ce588a95-7d89-4e58-a907-71364c01390f)

- **Pipeline ID:** `ce588a95-7d89-4e58-a907-71364c01390f`
- **Mode:** serverless
- **Source:** Databricks Asset Bundle at `network_analytics_pipeline/` ([databricks.yml](network_analytics_pipeline/databricks.yml))

### 3.1 Bronze — raw schema-typed

| Table | Source notebook | Source file | Expectations |
| --- | --- | --- | --- |
| [`bronze_fcc_bdc_h3`](https://fevm-cmegdemos.cloud.databricks.com/explore/data/cmegdemos_catalog/network_analytics_enablement/bronze_fcc_bdc_h3) | [bronze_fcc_bdc_h3.py](network_analytics_pipeline/src/bronze/bronze_fcc_bdc_h3.py) | `bdc_53_5GNR_mobile_broadband_h3_J25_14apr2026.zip` (GeoPackage) | `valid_fid` · `parsable_h3` (drop) · `known_technology` |
| [`bronze_building_footprints`](https://fevm-cmegdemos.cloud.databricks.com/explore/data/cmegdemos_catalog/network_analytics_enablement/bronze_building_footprints) | [bronze_building_footprints.py](network_analytics_pipeline/src/bronze/bronze_building_footprints.py) | `Washington.zip` (Shapefile) | `wkt_present` (drop) · `polygon_format` (drop) · `non_negative_height` |
| [`bronze_cell_towers`](https://fevm-cmegdemos.cloud.databricks.com/explore/data/cmegdemos_catalog/network_analytics_enablement/bronze_cell_towers) | [bronze_cell_towers.py](network_analytics_pipeline/src/bronze/bronze_cell_towers.py) | `310.csv.gz` (OpenCellID) | `non_null_cell` (drop) · `valid_mcc_310` (drop) · `valid_lat_lon` (drop) |

### 3.2 Silver — Seattle bbox, GEOMETRY(4326), carrier filter

| Table | Source notebook | Expectations |
| --- | --- | --- |
| [`silver_fcc_bdc_h3_seattle`](https://fevm-cmegdemos.cloud.databricks.com/explore/data/cmegdemos_catalog/network_analytics_enablement/silver_fcc_bdc_h3_seattle) | [silver_fcc_bdc_h3_seattle.py](network_analytics_pipeline/src/silver/silver_fcc_bdc_h3_seattle.py) | `in_seattle_bbox` (drop) · `known_5g_technology` · `non_null_speeds` |
| [`silver_building_footprints_seattle`](https://fevm-cmegdemos.cloud.databricks.com/explore/data/cmegdemos_catalog/network_analytics_enablement/silver_building_footprints_seattle) | [silver_building_footprints_seattle.py](network_analytics_pipeline/src/silver/silver_building_footprints_seattle.py) | `valid_geometry` (drop) · `centroid_in_seattle_bbox` (drop) · `non_negative_height` |
| [`silver_tmobile_towers_seattle`](https://fevm-cmegdemos.cloud.databricks.com/explore/data/cmegdemos_catalog/network_analytics_enablement/silver_tmobile_towers_seattle) | [silver_tmobile_towers_seattle.py](network_analytics_pipeline/src/silver/silver_tmobile_towers_seattle.py) | `tmobile_only` (**fail** if violated) · `point_geometry` (**fail**) · `in_seattle_bbox` (drop) · `radius_positive` |

### 3.3 Gold — analysis-ready

| Table | Source notebook | Expectations |
| --- | --- | --- |
| [`gold_downtown_building_coverage`](https://fevm-cmegdemos.cloud.databricks.com/explore/data/cmegdemos_catalog/network_analytics_enablement/gold_downtown_building_coverage) | [gold_downtown_building_coverage.py](network_analytics_pipeline/src/gold/gold_downtown_building_coverage.py) | `non_negative_speeds` (**fail**) · `reasonable_distance` 0–50 km (**fail**) · `valid_h3` (**fail**) · `has_nearest_tower` (drop) · `has_5g_coverage` |
| [`gold_coverage_by_distance_bucket`](https://fevm-cmegdemos.cloud.databricks.com/explore/data/cmegdemos_catalog/network_analytics_enablement/gold_coverage_by_distance_bucket) | [gold_coverage_by_distance_bucket.py](network_analytics_pipeline/src/gold/gold_coverage_by_distance_bucket.py) | `bucket_has_buildings` (**fail**) · `non_negative_avg_speed` (**fail**) · `non_null_avg_speed` |

**Expectation severity legend** — `expect` (warn-only), `expect_or_drop` (drop violating rows), `expect_or_fail` (abort the update).

### 3.4 Reference notebooks (alternative imperative path)

The same pipeline can be expressed as plain notebooks (bypassing DLT):

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
| SA2 | *"How do I trust the data?"* | KA → K1, K3 (governance + challenge process). Then point at the DLT pipeline expectations table in §3 |
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

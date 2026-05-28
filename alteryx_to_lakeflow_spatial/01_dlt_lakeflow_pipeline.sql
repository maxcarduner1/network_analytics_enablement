-- Databricks notebook source
-- MAGIC %md
-- MAGIC # LoRa — Lakeflow Declarative Pipeline (Designer canvas)
-- MAGIC
-- MAGIC This is the file that **Lakeflow Designer** opens as a visual canvas.
-- MAGIC Each `CREATE OR REFRESH MATERIALIZED VIEW` is one node on the canvas.
-- MAGIC Edges between nodes are inferred from the `FROM` clauses.
-- MAGIC
-- MAGIC | Node | Alteryx equivalent |
-- MAGIC |---|---|
-- MAGIC | `sites_silver` | Formula tool — `COALESCE(SITE_LATITUDE, RING_LATITUDE)` |
-- MAGIC | `markets_silver` | (passthrough) — keeps polygon WKT |
-- MAGIC | `sites_in_market` | Spatial Match — `ST_Contains` point-in-polygon |
-- MAGIC | `sites_nearest_market` | Find Nearest — `ST_DistanceSphere` + window |
-- MAGIC | `site_market_enriched` | Browse / Output — unified gold table |
-- MAGIC
-- MAGIC ### Parameterization
-- MAGIC
-- MAGIC The bronze tables are referenced via `${bronze_catalog}.${bronze_schema}.*`. When you
-- MAGIC **create the Lakeflow pipeline** that uses this file, set these two configuration
-- MAGIC values in the pipeline's "Advanced → Configuration" section:
-- MAGIC
-- MAGIC | Key | Value |
-- MAGIC |---|---|
-- MAGIC | `bronze_catalog` | the catalog you ran `00_bronze_ingestion` against (e.g., `cmegdemos_catalog`) |
-- MAGIC | `bronze_schema`  | the schema you ran `00_bronze_ingestion` against (e.g., `network_analytics_enablement`) |
-- MAGIC
-- MAGIC The pipeline's own **destination catalog/schema** (where silver/gold tables land) is set
-- MAGIC on the pipeline definition itself — not in this file.

-- COMMAND ----------

-- Silver: cell sites with COALESCE formula (Alteryx Formula tool)
CREATE OR REFRESH MATERIALIZED VIEW sites_silver
COMMENT "Cleaned site coordinates; mirrors the Alteryx Formula tool (COALESCE site/ring lat-long)."
AS
SELECT
  site_id,
  CAST(SITE_LATITUDE  AS DOUBLE) AS SITE_LATITUDE,
  CAST(SITE_LONGITUDE AS DOUBLE) AS SITE_LONGITUDE,
  CAST(SITE_LATITUDE  AS DOUBLE) AS S_SITE_LATITUDE,
  CAST(SITE_LONGITUDE AS DOUBLE) AS S_SITE_LONGITUDE
FROM ${bronze_catalog}.${bronze_schema}.sites_bronze
WHERE SITE_LATITUDE IS NOT NULL AND SITE_LONGITUDE IS NOT NULL;

-- COMMAND ----------

-- Silver: market polygons (passthrough — keep geometry as WKT for portability)
CREATE OR REFRESH MATERIALIZED VIEW markets_silver
COMMENT "Small/Rural county markets with polygon WKT geometry."
AS
SELECT
  ID                  AS market_id,
  Name                AS market_name,
  State               AS state,
  MarketType          AS market_type,
  MarketTypeDetailed  AS market_type_detailed,
  Population          AS population,
  geometry_wkt
FROM ${bronze_catalog}.${bronze_schema}.county_markets_bronze
WHERE geometry_wkt IS NOT NULL;

-- COMMAND ----------

-- Spatial Match: point-in-polygon (Alteryx "Spatial Match: target within universe")
CREATE OR REFRESH MATERIALIZED VIEW sites_in_market
COMMENT "Sites whose point falls inside a market polygon (ST_Contains)."
AS
SELECT
  s.site_id,
  s.S_SITE_LATITUDE,
  s.S_SITE_LONGITUDE,
  m.market_id,
  m.market_name,
  m.state,
  m.market_type_detailed,
  m.population
FROM LIVE.sites_silver s
JOIN LIVE.markets_silver m
  ON ST_Contains(
       ST_GeomFromWKT(m.geometry_wkt),
       ST_Point(s.S_SITE_LONGITUDE, s.S_SITE_LATITUDE)
     );

-- COMMAND ----------

-- Find Nearest: for sites outside every polygon (Alteryx "Find Nearest" tool)
CREATE OR REFRESH MATERIALIZED VIEW sites_nearest_market
COMMENT "For sites with no containing market, the closest market by sphere distance."
AS
WITH unmatched AS (
  SELECT s.*
  FROM LIVE.sites_silver s
  LEFT ANTI JOIN LIVE.sites_in_market m USING (site_id)
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
  CROSS JOIN LIVE.markets_silver m
)
SELECT site_id, S_SITE_LATITUDE, S_SITE_LONGITUDE,
       market_id, market_name, state, market_type_detailed, population,
       ROUND(distance_m / 1000.0, 2) AS distance_km
FROM ranked
WHERE rn = 1;

-- COMMAND ----------

-- Gold: unified output (Alteryx Browse / final Output Data tool)
CREATE OR REFRESH MATERIALIZED VIEW site_market_enriched
COMMENT "Every site joined to its market — either contained WITHIN it or NEAREST to it."
AS
SELECT site_id, S_SITE_LATITUDE, S_SITE_LONGITUDE,
       market_id, market_name, state, market_type_detailed, population,
       'WITHIN'  AS match_type,
       0.0       AS distance_km
FROM LIVE.sites_in_market
UNION ALL
SELECT site_id, S_SITE_LATITUDE, S_SITE_LONGITUDE,
       market_id, market_name, state, market_type_detailed, population,
       'NEAREST' AS match_type,
       distance_km
FROM LIVE.sites_nearest_market;

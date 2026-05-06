-- One-time migration: SDP cannot change dataset type in place from
-- MATERIALIZED_VIEW -> STREAMING_TABLE.
--
-- Run this if ops_app bronze tables existed before the Auto Loader conversion.

DROP TABLE IF EXISTS cmegdemos_catalog.network_analytics_enablement.ops_app_bronze_tower_hourly_kpis;
DROP TABLE IF EXISTS cmegdemos_catalog.network_analytics_enablement.ops_app_bronze_building_hourly_demand;

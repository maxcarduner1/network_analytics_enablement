-- One-time migration: SDP cannot change bronze_cell_towers from MATERIALIZED_VIEW to STREAMING_TABLE in place.
-- Run in a SQL warehouse / notebook against the target catalog.schema, then deploy and run the pipeline again.
--
-- Error you are fixing: CANNOT_CHANGE_DATASET_TYPE / "Cannot change the dataset type ... from MATERIALIZED_VIEW to STREAMING_TABLE"
--
-- Replace catalog/schema if your bundle variables differ.

DROP TABLE IF EXISTS cmegdemos_catalog.network_analytics_enablement.bronze_cell_towers;

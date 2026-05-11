"""Gold: merge baseline coverage with synthetic ops telemetry for demo use."""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession


@dp.materialized_view(
    name="ops_app_gold_downtown_building_coverage",
    comment=(
        "Ops-enriched downtown Seattle coverage table that merges baseline building "
        "coverage with synthetic demand and nearest-tower KPI telemetry."
    ),
    cluster_by=["h3_res9_id", "nearest_tower_id"],
)
@dp.expect_or_fail("non_negative_download", "best_download_mbps >= 0")
@dp.expect_or_drop("has_building_id", "building_id IS NOT NULL")
@dp.expect_or_drop("has_nearest_tower", "nearest_tower_id IS NOT NULL")
def ops_app_gold_downtown_building_coverage():
    spark = SparkSession.builder.getOrCreate()
    return spark.sql(
        """
        SELECT
            g.building_id,
            g.building_height,
            g.geometry,
            g.centroid_lon,
            g.centroid_lat,
            g.h3_res9_id,
            g.best_download_mbps,
            g.best_upload_mbps,
            g.provider_count,
            g.nearest_tower_id,
            g.nearest_carrier,
            g.nearest_tower_type,
            g.nearest_cell_id,
            g.nearest_coverage_radius_m,
            g.nearest_tower_samples,
            g.distance_to_tower_m,
            d.demand_snapshot_ts,
            d.demand_users,
            d.traffic_mix,
            d.indoor_penetration_factor,
            d.effective_radio_load_score,
            k.kpi_snapshot_ts,
            k.prb_utilization_pct,
            k.latency_ms,
            k.packet_loss_pct,
            k.throughput_mbps,
            k.tower_health_band,
            CASE
                WHEN k.tower_health_band = 'critical' OR d.effective_radio_load_score >= 45 THEN 'critical'
                WHEN k.tower_health_band = 'watch' OR d.effective_radio_load_score >= 28 THEN 'watch'
                ELSE 'healthy'
            END AS building_service_risk_band
        FROM gold_downtown_building_coverage g
        LEFT JOIN ops_app_silver_building_demand_latest d
            ON g.building_id = d.building_id
        LEFT JOIN ops_app_silver_tower_kpis_latest k
            ON g.nearest_tower_id = k.tower_id
        """
    )

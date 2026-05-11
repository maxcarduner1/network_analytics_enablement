"""Silver: latest synthetic KPI state per Seattle T-Mobile tower."""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession


@dp.materialized_view(
    name="ops_app_silver_tower_kpis_latest",
    comment=(
        "Latest synthetic KPI snapshot per Seattle T-Mobile tower derived from "
        "hourly KPI events."
    ),
    cluster_by=["tower_id"],
)
@dp.expect_or_drop("valid_utilization", "prb_utilization_pct BETWEEN 0 AND 100")
@dp.expect_or_drop("valid_packet_loss", "packet_loss_pct BETWEEN 0 AND 100")
@dp.expect_or_drop("valid_throughput", "throughput_mbps >= 0")
def ops_app_silver_tower_kpis_latest():
    spark = SparkSession.builder.getOrCreate()
    return spark.sql(
        """
        WITH ranked AS (
            SELECT
                tower_id,
                event_ts,
                prb_utilization_pct,
                latency_ms,
                packet_loss_pct,
                throughput_mbps,
                ROW_NUMBER() OVER (
                    PARTITION BY tower_id
                    ORDER BY event_ts DESC
                ) AS rn
            FROM ops_app_bronze_tower_hourly_kpis
        )
        SELECT
            tower_id,
            event_ts AS kpi_snapshot_ts,
            prb_utilization_pct,
            latency_ms,
            packet_loss_pct,
            throughput_mbps,
            CASE
                WHEN prb_utilization_pct >= 85 OR latency_ms >= 120 OR packet_loss_pct >= 3 THEN 'critical'
                WHEN prb_utilization_pct >= 70 OR latency_ms >= 90  OR packet_loss_pct >= 1.5 THEN 'watch'
                ELSE 'healthy'
            END AS tower_health_band
        FROM ranked
        WHERE rn = 1
        """
    )

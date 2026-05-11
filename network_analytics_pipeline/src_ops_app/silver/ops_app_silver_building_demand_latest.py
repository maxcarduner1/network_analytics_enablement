"""Silver: latest synthetic demand state per downtown building."""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession


@dp.materialized_view(
    name="ops_app_silver_building_demand_latest",
    comment=(
        "Latest synthetic demand snapshot per downtown building derived from "
        "hourly demand events."
    ),
    cluster_by=["building_id"],
)
@dp.expect_or_drop("valid_demand_users", "demand_users >= 0")
@dp.expect("valid_mix", "traffic_mix IN ('video_heavy', 'commute', 'balanced')")
def ops_app_silver_building_demand_latest():
    spark = SparkSession.builder.getOrCreate()
    return spark.sql(
        """
        WITH ranked AS (
            SELECT
                building_id,
                event_ts,
                demand_users,
                traffic_mix,
                indoor_penetration_factor,
                ROW_NUMBER() OVER (
                    PARTITION BY building_id
                    ORDER BY event_ts DESC
                ) AS rn
            FROM ops_app_bronze_building_hourly_demand
        )
        SELECT
            building_id,
            event_ts AS demand_snapshot_ts,
            demand_users,
            traffic_mix,
            indoor_penetration_factor,
            ROUND(
                demand_users * (1.1 - indoor_penetration_factor / 2.0),
                2
            ) AS effective_radio_load_score
        FROM ranked
        WHERE rn = 1
        """
    )

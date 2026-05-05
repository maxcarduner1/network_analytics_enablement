"""Bronze: synthetic hourly demand events per downtown Seattle building."""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession


@dp.materialized_view(
    name="ops_app_bronze_building_hourly_demand",
    comment=(
        "Synthetic building-level hourly demand events for the last 14 days, "
        "derived from downtown Seattle buildings. Raw event grain for ops demos."
    ),
    cluster_by=["building_id", "event_ts"],
)
@dp.expect_or_drop("building_id_not_null", "building_id IS NOT NULL")
@dp.expect_or_drop("non_negative_demand_users", "demand_users >= 0")
@dp.expect_or_drop("indoor_penetration_in_range", "indoor_penetration_factor BETWEEN 0.4 AND 1.0")
def ops_app_bronze_building_hourly_demand():
    spark = SparkSession.builder.getOrCreate()
    return spark.sql(
        """
        WITH hours AS (
            SELECT explode(
                sequence(
                    date_trunc('hour', current_timestamp() - INTERVAL 14 DAYS),
                    date_trunc('hour', current_timestamp()),
                    INTERVAL 1 HOUR
                )
            ) AS event_ts
        )
        SELECT
            b.building_id,
            h.event_ts,
            CAST(
                GREATEST(
                    1,
                    ROUND(
                        (
                            8 + (COALESCE(b.building_height, 20.0) / 8.0)
                        ) *
                        CASE
                            WHEN hour(h.event_ts) BETWEEN 7 AND 10 THEN 2.2
                            WHEN hour(h.event_ts) BETWEEN 16 AND 20 THEN 2.6
                            WHEN hour(h.event_ts) BETWEEN 0 AND 5 THEN 0.45
                            ELSE 1.0
                        END *
                        CASE
                            WHEN dayofweek(h.event_ts) IN (1, 7) THEN 0.7
                            ELSE 1.0
                        END *
                        (0.9 + rand() * 0.35)
                    )
                ) AS INT
            ) AS demand_users,
            CASE
                WHEN hour(h.event_ts) BETWEEN 18 AND 23 THEN 'video_heavy'
                WHEN hour(h.event_ts) BETWEEN 7 AND 9 THEN 'commute'
                ELSE 'balanced'
            END AS traffic_mix,
            ROUND(0.45 + rand() * 0.5, 3) AS indoor_penetration_factor
        FROM gold_downtown_building_coverage b
        CROSS JOIN hours h
        """
    )

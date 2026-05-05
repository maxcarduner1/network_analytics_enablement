"""Bronze: synthetic hourly tower KPI events for Seattle T-Mobile towers."""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession


@dp.materialized_view(
    name="ops_app_bronze_tower_hourly_kpis",
    comment=(
        "Synthetic tower-level hourly KPIs for the last 14 days, generated from "
        "Seattle T-Mobile towers at raw event grain."
    ),
    cluster_by=["tower_id", "event_ts"],
)
@dp.expect_or_drop("tower_id_not_null", "tower_id IS NOT NULL")
@dp.expect_or_drop("utilization_in_bounds", "prb_utilization_pct BETWEEN 0 AND 100")
@dp.expect_or_drop("latency_positive", "latency_ms > 0")
def ops_app_bronze_tower_hourly_kpis():
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
            t.tower_id,
            h.event_ts,
            ROUND(
                LEAST(
                    100.0,
                    GREATEST(
                        5.0,
                        35
                        + CASE
                            WHEN hour(h.event_ts) BETWEEN 7 AND 10 THEN 28
                            WHEN hour(h.event_ts) BETWEEN 17 AND 21 THEN 33
                            WHEN hour(h.event_ts) BETWEEN 0 AND 5 THEN -12
                            ELSE 0
                          END
                        + (rand() * 24 - 12)
                    )
                ),
                2
            ) AS prb_utilization_pct,
            ROUND(
                GREATEST(
                    10.0,
                    160
                    - (COALESCE(t.avg_signal, -95) + 110) * 2.0
                    + CASE
                        WHEN hour(h.event_ts) BETWEEN 17 AND 21 THEN 18
                        ELSE 0
                      END
                    + (rand() * 16 - 8)
                ),
                2
            ) AS latency_ms,
            ROUND(
                GREATEST(
                    0.0,
                    LEAST(
                        18.0,
                        0.6
                        + CASE
                            WHEN hour(h.event_ts) BETWEEN 17 AND 21 THEN 2.2
                            ELSE 0.8
                          END
                        + (rand() * 1.4)
                    )
                ),
                3
            ) AS packet_loss_pct,
            ROUND(
                GREATEST(
                    20.0,
                    240.0
                    - (
                        LEAST(
                            100.0,
                            GREATEST(
                                5.0,
                                35
                                + CASE
                                    WHEN hour(h.event_ts) BETWEEN 7 AND 10 THEN 28
                                    WHEN hour(h.event_ts) BETWEEN 17 AND 21 THEN 33
                                    WHEN hour(h.event_ts) BETWEEN 0 AND 5 THEN -12
                                    ELSE 0
                                  END
                                + (rand() * 24 - 12)
                            )
                        )
                    ) * 1.2
                    + (rand() * 15 - 7.5)
                ),
                2
            ) AS throughput_mbps
        FROM silver_tmobile_towers_seattle t
        CROSS JOIN hours h
        """
    )

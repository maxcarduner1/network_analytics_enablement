"""Gold: distance-to-nearest-tower vs. 5G NR signal-strength aggregation.

Buckets buildings by their distance to the nearest T-Mobile tower and reports
the average download/upload speed and provider count per bucket. Mirrors the
distance-vs-signal analysis in notebook 02_Analysis.ipynb.
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession


@dp.materialized_view(
    name="gold_coverage_by_distance_bucket",
    comment=(
        "5G NR coverage quality bucketed by distance to the nearest T-Mobile "
        "tower for downtown-Seattle buildings with non-zero coverage. Useful for "
        "validating that signal strength degrades smoothly with distance."
    ),
)
@dp.expect_or_fail("bucket_has_buildings", "buildings > 0")
@dp.expect_or_fail("non_negative_avg_speed", "avg_download_mbps >= 0")
@dp.expect("non_null_avg_speed", "avg_download_mbps IS NOT NULL")
def gold_coverage_by_distance_bucket():
    spark = SparkSession.builder.getOrCreate()
    return spark.sql(
        """
        SELECT
            CASE
                WHEN distance_to_tower_m < 200  THEN '1: < 200m'
                WHEN distance_to_tower_m < 500  THEN '2: 200-500m'
                WHEN distance_to_tower_m < 1000 THEN '3: 500m-1km'
                WHEN distance_to_tower_m < 2000 THEN '4: 1-2km'
                ELSE                                 '5: > 2km'
            END                                              AS distance_bucket,
            COUNT(*)                                         AS buildings,
            ROUND(AVG(best_download_mbps), 1)                AS avg_download_mbps,
            ROUND(AVG(best_upload_mbps),   1)                AS avg_upload_mbps,
            ROUND(AVG(distance_to_tower_m), 0)               AS avg_distance_m,
            ROUND(AVG(provider_count),      1)               AS avg_providers
        FROM gold_downtown_building_coverage
        WHERE best_download_mbps > 0
        GROUP BY 1
        """
    )

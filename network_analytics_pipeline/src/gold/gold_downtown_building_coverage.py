"""Gold: downtown Seattle buildings × 5G NR coverage × nearest T-Mobile tower.

Joins all three silver tables into the business-ready table consumed by the
notebook 02 analysis: one row per downtown building with its best available
5G download/upload speed (from the FCC BDC H3 hex it falls into), the H3 cell
ID, and the nearest T-Mobile tower with the haversine distance in meters.
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

# Downtown Seattle core bbox (matches notebook 02_Analysis.ipynb).
LAT_MIN, LAT_MAX = 47.595, 47.625
LON_MIN, LON_MAX = -122.355, -122.325


@dp.materialized_view(
    name="gold_downtown_building_coverage",
    comment=(
        "Downtown Seattle buildings joined with FCC BDC 5G NR coverage on H3 res-9 "
        "and the nearest T-Mobile cell tower (haversine distance). One row per "
        "building, ready for visualization and ML."
    ),
    cluster_by=["h3_res9_id"],
)
@dp.expect_or_fail(
    "non_negative_speeds",
    "best_download_mbps >= 0 AND best_upload_mbps >= 0",
)
@dp.expect_or_fail(
    "reasonable_distance",
    "distance_to_tower_m BETWEEN 0 AND 50000",
)
@dp.expect_or_fail("valid_h3", "h3_isvalid(h3_res9_id)")
@dp.expect_or_drop("has_nearest_tower", "nearest_tower_id IS NOT NULL")
@dp.expect("has_5g_coverage", "best_download_mbps > 0")
def gold_downtown_building_coverage():
    spark = SparkSession.builder.getOrCreate()
    return spark.sql(
        f"""
        WITH bdc_per_hex AS (
            SELECT
                h3_res9_id,
                MAX(mindown) AS best_download_mbps,
                MAX(minup)   AS best_upload_mbps,
                COUNT(*)     AS provider_count
            FROM silver_fcc_bdc_h3_seattle
            GROUP BY h3_res9_id
        ),
        downtown_bldg AS (
            SELECT
                building_id,
                height AS building_height,
                geometry,
                ST_Centroid(geometry)            AS centroid,
                ST_X(ST_Centroid(geometry))      AS centroid_lon,
                ST_Y(ST_Centroid(geometry))      AS centroid_lat,
                h3_h3tostring(
                    h3_longlatash3(
                        ST_X(ST_Centroid(geometry)),
                        ST_Y(ST_Centroid(geometry)),
                        9
                    )
                ) AS h3_res9_id
            FROM silver_building_footprints_seattle
            WHERE ST_Y(ST_Centroid(geometry)) BETWEEN {LAT_MIN} AND {LAT_MAX}
              AND ST_X(ST_Centroid(geometry)) BETWEEN {LON_MIN} AND {LON_MAX}
        ),
        nearby_towers AS (
            SELECT
                tower_id,
                carrier,
                tower_type,
                cell_id,
                coverage_radius_m,
                samples,
                location
            FROM silver_tmobile_towers_seattle
            WHERE latitude  BETWEEN {LAT_MIN - 0.03} AND {LAT_MAX + 0.03}
              AND longitude BETWEEN {LON_MIN - 0.03} AND {LON_MAX + 0.03}
        ),
        ranked AS (
            SELECT
                b.building_id,
                t.tower_id           AS nearest_tower_id,
                t.carrier            AS nearest_carrier,
                t.tower_type         AS nearest_tower_type,
                t.cell_id            AS nearest_cell_id,
                t.coverage_radius_m  AS nearest_coverage_radius_m,
                t.samples            AS nearest_tower_samples,
                ROUND(ST_DistanceSphere(b.centroid, t.location), 1) AS distance_to_tower_m,
                ROW_NUMBER() OVER (
                    PARTITION BY b.building_id
                    ORDER BY ST_DistanceSphere(b.centroid, t.location)
                ) AS rn
            FROM downtown_bldg b
            CROSS JOIN nearby_towers t
        )
        SELECT
            b.building_id,
            b.building_height,
            b.geometry,
            b.centroid_lon,
            b.centroid_lat,
            b.h3_res9_id,
            COALESCE(c.best_download_mbps, 0) AS best_download_mbps,
            COALESCE(c.best_upload_mbps,   0) AS best_upload_mbps,
            COALESCE(c.provider_count,     0) AS provider_count,
            r.nearest_tower_id,
            r.nearest_carrier,
            r.nearest_tower_type,
            r.nearest_cell_id,
            r.nearest_coverage_radius_m,
            r.nearest_tower_samples,
            r.distance_to_tower_m
        FROM downtown_bldg b
        JOIN ranked        r ON b.building_id = r.building_id AND r.rn = 1
        LEFT JOIN bdc_per_hex c ON b.h3_res9_id = c.h3_res9_id
        """
    )

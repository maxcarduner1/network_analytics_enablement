"""Silver: T-Mobile cell towers inside the Seattle metro with POINT geometry.

Restricts the bronze OpenCellID feed to T-Mobile's MNC (260) inside the Seattle
metro bbox, attaches a stable `tower_id`, and materializes a native POINT(4326)
location column ready for spatial joins downstream.
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

LAT_MIN, LAT_MAX = 47.40, 47.80
LON_MIN, LON_MAX = -122.50, -122.10
TMOBILE_MNC = 260


@dp.materialized_view(
    name="silver_tmobile_towers_seattle",
    comment=(
        "T-Mobile (MNC 260) cell towers from OpenCellID inside the Seattle metro "
        "bbox, with a native POINT(4326) `location` column for spatial joins."
    ),
)
@dp.expect_or_fail("tmobile_only", f"net = {TMOBILE_MNC}")
@dp.expect_or_fail("point_geometry", "ST_GeometryType(location) = 'ST_Point'")
@dp.expect_or_drop(
    "in_seattle_bbox",
    f"latitude BETWEEN {LAT_MIN} AND {LAT_MAX} "
    f"AND longitude BETWEEN {LON_MIN} AND {LON_MAX}",
)
@dp.expect("radius_positive", "coverage_radius_m IS NULL OR coverage_radius_m > 0")
def silver_tmobile_towers_seattle():
    spark = SparkSession.builder.getOrCreate()
    return spark.read.table("bronze_cell_towers").selectExpr(
        "monotonically_increasing_id() AS tower_id",
        "'T-Mobile' AS carrier",
        "radio AS tower_type",
        "net",
        "area AS lac_tac",
        "cell AS cell_id",
        "range_m AS coverage_radius_m",
        "samples",
        "lat AS latitude",
        "lon AS longitude",
        "created_ts",
        "updated_ts",
        "avg_signal",
        "ST_Point(lon, lat, 4326) AS location",
    ).where(
        f"net = {TMOBILE_MNC} "
        f"AND lat BETWEEN {LAT_MIN} AND {LAT_MAX} "
        f"AND lon BETWEEN {LON_MIN} AND {LON_MAX}"
    )

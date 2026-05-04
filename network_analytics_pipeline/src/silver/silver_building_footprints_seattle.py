"""Silver: building footprints in the Seattle metro with native GEOMETRY(4326).

Promotes the bronze WKT into a Databricks native GEOMETRY type, assigns a stable
`building_id`, and trims to the Seattle bounding box using the polygon centroid.
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

LAT_MIN, LAT_MAX = 47.40, 47.80
LON_MIN, LON_MAX = -122.50, -122.10


@dp.materialized_view(
    name="silver_building_footprints_seattle",
    comment=(
        "Microsoft building-footprint polygons promoted to GEOMETRY(4326) and "
        "filtered to the Seattle metro bounding box (centroid-based). One row "
        "per building with a stable building_id and the original height."
    ),
)
@dp.expect_or_drop("valid_geometry", "ST_IsValid(geometry)")
@dp.expect_or_drop(
    "centroid_in_seattle_bbox",
    f"ST_Y(ST_Centroid(geometry)) BETWEEN {LAT_MIN} AND {LAT_MAX} "
    f"AND ST_X(ST_Centroid(geometry)) BETWEEN {LON_MIN} AND {LON_MAX}",
)
@dp.expect("non_negative_height", "height IS NULL OR height >= 0")
def silver_building_footprints_seattle():
    spark = SparkSession.builder.getOrCreate()
    return spark.read.table("bronze_building_footprints").selectExpr(
        "monotonically_increasing_id() AS building_id",
        "CAST(height AS DOUBLE) AS height",
        "ST_GeomFromText(wkt, 4326) AS geometry",
    )

"""Silver: FCC BDC 5G NR coverage hexagons restricted to the Seattle metro.

Computes the WGS84 lat/lon of each H3 cell center using the Databricks H3 +
spatial SQL functions and keeps only rows whose center falls inside the Seattle
metro bounding box.
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

# Seattle metro bounding box (matches the value used in 01_Ingest.ipynb).
LAT_MIN, LAT_MAX = 47.40, 47.80
LON_MIN, LON_MAX = -122.50, -122.10


@dp.materialized_view(
    name="silver_fcc_bdc_h3_seattle",
    comment=(
        "FCC BDC 5G NR mobile-broadband H3 res-9 hexagons whose cell centers fall "
        "inside the Seattle metro bounding box. The H3 cell center coordinates "
        "are derived with Databricks H3 + ST functions and surfaced as columns."
    ),
    cluster_by=["h3_res9_id"],
)
@dp.expect_or_drop(
    "in_seattle_bbox",
    f"center_lat BETWEEN {LAT_MIN} AND {LAT_MAX} "
    f"AND center_lon BETWEEN {LON_MIN} AND {LON_MAX}",
)
@dp.expect("known_5g_technology", "technology IS NOT NULL")
@dp.expect("non_null_speeds", "mindown IS NOT NULL AND minup IS NOT NULL")
def silver_fcc_bdc_h3_seattle():
    spark = SparkSession.builder.getOrCreate()
    return spark.read.table("bronze_fcc_bdc_h3").selectExpr(
        "fid",
        "technology",
        "mindown",
        "minup",
        "environmnt",
        "h3_res9_id",
        "ST_Y(ST_GeomFromWKB(h3_centeraswkb(h3_res9_id))) AS center_lat",
        "ST_X(ST_GeomFromWKB(h3_centeraswkb(h3_res9_id))) AS center_lon",
    )

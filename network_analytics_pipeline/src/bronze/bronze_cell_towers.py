"""Bronze: raw OpenCellID cell-tower records for the USA (MCC=310).

Reads the headerless gzipped CSV directly with Spark — no zip extraction needed.
Casts numeric columns to typed forms but keeps the full USA row set; silver
narrows to T-Mobile (MNC 260) inside the Seattle bbox and adds a POINT geometry.
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime
from pyspark.sql.types import DoubleType, IntegerType, LongType

CSV_NAME = "310.csv.gz"

OPENCELLID_COLUMNS = [
    "radio",
    "mcc",
    "net",
    "area",
    "cell",
    "unit",
    "lon",
    "lat",
    "range_m",
    "samples",
    "changeable",
    "created",
    "updated",
    "avg_signal",
]


@dp.materialized_view(
    name="bronze_cell_towers",
    comment=(
        "Raw OpenCellID cell-tower records for the USA (MCC 310). Read from a "
        "gzipped CSV on a Unity Catalog Volume with native Spark IO; columns are "
        "typed but no carrier or geographic filter is applied at this layer."
    ),
)
@dp.expect_or_drop("non_null_cell", "cell IS NOT NULL")
@dp.expect_or_drop("valid_mcc_310", "mcc = 310")
@dp.expect_or_drop(
    "valid_lat_lon",
    "lat BETWEEN -90 AND 90 AND lon BETWEEN -180 AND 180",
)
def bronze_cell_towers():
    spark = SparkSession.builder.getOrCreate()
    volume_path = spark.conf.get(
        "pipeline.raw_volume_path",
        "/Volumes/cmegdemos_catalog/network_analytics_enablement/raw_data",
    )

    raw = (
        spark.read.format("csv")
        .option("header", "false")
        .load(f"{volume_path}/{CSV_NAME}")
        .toDF(*OPENCELLID_COLUMNS)
    )

    return raw.select(
        col("radio"),
        col("mcc").cast(IntegerType()).alias("mcc"),
        col("net").cast(IntegerType()).alias("net"),
        col("area").cast(IntegerType()).alias("area"),
        col("cell").cast(LongType()).alias("cell"),
        col("unit").cast(IntegerType()).alias("unit"),
        col("lon").cast(DoubleType()).alias("lon"),
        col("lat").cast(DoubleType()).alias("lat"),
        col("range_m").cast(IntegerType()).alias("range_m"),
        col("samples").cast(IntegerType()).alias("samples"),
        from_unixtime(col("created").cast(LongType())).alias("created_ts"),
        from_unixtime(col("updated").cast(LongType())).alias("updated_ts"),
        col("avg_signal").cast(IntegerType()).alias("avg_signal"),
    )

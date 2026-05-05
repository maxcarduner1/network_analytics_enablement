"""Bronze: raw OpenCellID cell-tower records for the USA (MCC=310).

Ingests headerless gzipped CSV shards under ``<raw_volume_path>/<subdir>/`` using
Auto Loader (`cloudFiles`). Append-only: new files (e.g. dated drops) are
discovered on each pipeline update. Format matches the historical single-file
`310.csv.gz` layout (14 columns). Silver narrows to T-Mobile (MNC 260) inside
the Seattle bbox.
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime
from pyspark.sql.types import DoubleType, IntegerType, LongType

# Default subdir under `pipeline.raw_volume_path`; override via
# `pipeline.cell_towers_incoming_subdir` in the bundle.
INCOMING_SUBDIR_DEFAULT = "cell_towers"


@dp.table(
    name="bronze_cell_towers",
    comment=(
        "Raw OpenCellID cell-tower records for the USA (MCC 310). Ingested with "
        "Auto Loader from gzip CSV files under the configured incoming folder; "
        "append-only incremental file discovery."
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
    subdir = spark.conf.get(
        "pipeline.cell_towers_incoming_subdir",
        INCOMING_SUBDIR_DEFAULT,
    )
    incoming_path = f"{volume_path.rstrip('/')}/{subdir}"

    raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "false")
        .option("pathGlobFilter", "*.csv.gz")
        .option("recursiveFileLookup", "true")
        .load(incoming_path)
    )

    # Headerless CSV → default column names _c0 .. _c13
    return raw.select(
        col("_c0").alias("radio"),
        col("_c1").cast(IntegerType()).alias("mcc"),
        col("_c2").cast(IntegerType()).alias("net"),
        col("_c3").cast(IntegerType()).alias("area"),
        col("_c4").cast(LongType()).alias("cell"),
        col("_c5").cast(IntegerType()).alias("unit"),
        col("_c6").cast(DoubleType()).alias("lon"),
        col("_c7").cast(DoubleType()).alias("lat"),
        col("_c8").cast(IntegerType()).alias("range_m"),
        col("_c9").cast(IntegerType()).alias("samples"),
        col("_c10").cast(IntegerType()).alias("changeable"),
        from_unixtime(col("_c11").cast(LongType())).alias("created_ts"),
        from_unixtime(col("_c12").cast(LongType())).alias("updated_ts"),
        col("_c13").cast(IntegerType()).alias("avg_signal"),
    )

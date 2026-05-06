"""Bronze: ops_app building demand events ingested via Auto Loader."""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, LongType, StringType, TimestampType

INCOMING_SUBDIR_DEFAULT = "demand"


@dp.table(
    name="ops_app_bronze_building_hourly_demand",
    comment=(
        "Ops-app building demand events ingested with Auto Loader from gzip CSV "
        "shards under the configured incoming folder."
    ),
)
@dp.expect_or_drop("building_id_not_null", "building_id IS NOT NULL")
@dp.expect_or_drop("non_negative_demand_users", "demand_users >= 0")
@dp.expect_or_drop("indoor_penetration_in_range", "indoor_penetration_factor BETWEEN 0.4 AND 1.0")
def ops_app_bronze_building_hourly_demand():
    spark = SparkSession.builder.getOrCreate()
    volume_path = spark.conf.get(
        "pipeline.raw_volume_path",
        "/Volumes/cmegdemos_catalog/network_analytics_enablement/raw_data",
    )
    subdir = spark.conf.get(
        "pipeline.ops_app_demand_incoming_subdir",
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

    return raw.select(
        col("_c0").cast(LongType()).alias("building_id"),
        col("_c1").cast(TimestampType()).alias("event_ts"),
        col("_c2").cast(LongType()).alias("demand_users"),
        col("_c3").cast(StringType()).alias("traffic_mix"),
        col("_c4").cast(DoubleType()).alias("indoor_penetration_factor"),
    )

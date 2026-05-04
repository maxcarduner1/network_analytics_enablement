"""Bronze: raw FCC BDC 5G NR mobile broadband H3 hexagons.

Extracts the GeoPackage from a zip archive in the Unity Catalog Volume and
returns every Washington-state row with minimal validation. Silver applies the
Seattle bounding-box filter and projection.
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

import os
import shutil
import sqlite3
import tempfile
import zipfile

import pandas as pd

ZIP_NAME = "bdc_53_5GNR_mobile_broadband_h3_J25_14apr2026.zip"
GPKG_NAME = "bdc_53_5GNR_mobile_broadband_h3_J25_14apr2026.gpkg"
GPKG_TABLE = "bdc_53_5GNR_mobile_broadband_h3_J25_14apr2026"


@dp.materialized_view(
    name="bronze_fcc_bdc_h3",
    comment=(
        "Raw FCC Broadband Data Collection (BDC) 5G NR mobile-broadband H3 res-9 "
        "hexagons for Washington state. Extracted from a GeoPackage inside a zip "
        "archive on a Unity Catalog Volume; no projection or geographic filter "
        "applied at this layer."
    ),
)
@dp.expect("valid_fid", "fid IS NOT NULL")
@dp.expect_or_drop("parsable_h3", "h3_isvalid(h3_res9_id)")
@dp.expect("known_technology", "technology IS NOT NULL")
def bronze_fcc_bdc_h3():
    spark = SparkSession.builder.getOrCreate()
    volume_path = spark.conf.get(
        "pipeline.raw_volume_path",
        "/Volumes/cmegdemos_catalog/network_analytics_enablement/raw_data",
    )

    tmpdir = tempfile.mkdtemp(prefix="bdc_")
    try:
        with zipfile.ZipFile(f"{volume_path}/{ZIP_NAME}") as zf:
            zf.extractall(tmpdir)

        gpkg_path = os.path.join(tmpdir, GPKG_NAME)
        conn = sqlite3.connect(gpkg_path)
        try:
            pdf = pd.read_sql_query(
                f"""
                SELECT fid, technology, mindown, minup, environmnt, h3_res9_id
                FROM {GPKG_TABLE}
                """,
                conn,
            )
        finally:
            conn.close()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    return spark.createDataFrame(pdf)

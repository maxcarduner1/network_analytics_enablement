"""Bronze: raw Microsoft building footprints for Washington state.

Extracts the shapefile bundle from `Washington.zip` on a Unity Catalog Volume,
walks every polygon record with `pyshp`, and emits the polygon as a WKT string
plus the original height attribute. No CRS conversion or geographic filter is
applied here; silver promotes the WKT into a native GEOMETRY(4326) and trims
to the Seattle metro bbox.
"""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

import os
import shutil
import tempfile
import zipfile

import pandas as pd
import shapefile

ZIP_NAME = "Washington.zip"
SHAPE_PREFIX = "bldg_footprints"


def _shape_to_wkt(shape) -> str | None:
    """Convert a pyshp polygon Shape into a POLYGON WKT string."""
    if shape.shapeType != 5:
        return None
    parts = list(shape.parts) + [len(shape.points)]
    rings = []
    for i in range(len(parts) - 1):
        ring_pts = shape.points[parts[i] : parts[i + 1]]
        if len(ring_pts) < 4:
            return None
        coords = ", ".join(f"{p[0]} {p[1]}" for p in ring_pts)
        rings.append(f"({coords})")
    return f"POLYGON({', '.join(rings)})" if rings else None


@dp.materialized_view(
    name="bronze_building_footprints",
    comment=(
        "Raw Microsoft building footprints for Washington state, extracted from a "
        "shapefile bundle on a Unity Catalog Volume. Polygons are emitted as WKT "
        "strings (no native GEOMETRY type yet) with the original height attribute."
    ),
)
@dp.expect_or_drop("wkt_present", "wkt IS NOT NULL")
@dp.expect_or_drop("polygon_format", "wkt LIKE 'POLYGON%'")
@dp.expect("non_negative_height", "height IS NULL OR height >= 0")
def bronze_building_footprints():
    spark = SparkSession.builder.getOrCreate()
    volume_path = spark.conf.get(
        "pipeline.raw_volume_path",
        "/Volumes/cmegdemos_catalog/network_analytics_enablement/raw_data",
    )

    tmpdir = tempfile.mkdtemp(prefix="bldg_")
    try:
        with zipfile.ZipFile(f"{volume_path}/{ZIP_NAME}") as zf:
            zf.extractall(tmpdir)

        shp_path = os.path.join(tmpdir, SHAPE_PREFIX)
        sf = shapefile.Reader(shp_path)
        records: list[dict] = []
        try:
            for sr in sf.iterShapeRecords():
                wkt = _shape_to_wkt(sr.shape)
                height_raw = sr.record[0] if sr.record else None
                try:
                    height = float(height_raw) if height_raw not in (None, "") else None
                except (TypeError, ValueError):
                    height = None
                records.append({"height": height, "wkt": wkt})
        finally:
            sf.close()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    pdf = pd.DataFrame(records)
    return spark.createDataFrame(pdf)

"""Shared notebook bootstrap.

The Jupyter image already applies the Spark defaults (Spline agent + Iceberg
catalog) via `spark-defaults.conf`, so callers can stay minimal:

    from _shared.spark_session import get_spark
    spark = get_spark()
"""

from __future__ import annotations

from pyspark.sql import SparkSession

_NAME = "spark-lineage"


def get_spark() -> SparkSession:
    """Return a singleton SparkSession configured for Spline + Iceberg."""
    return (
        SparkSession.builder.appName(_NAME)
        # ponytail: defensive re-declares the listener in case someone clears
        # spark-defaults.conf when running standalone.
        .config(
            "spark.sql.queryExecutionListeners",
            "za.co.absa.spline.harvester.listener.SplineQueryExecutionListener",
        )
        .config("spark.spline.lineageDispatcher.http.producer.url", "http://spline-rest:8080/producer")
        .getOrCreate()
    )


DATA_DIR = "/home/jovyan/data"
SAMPLE_CSV = f"{DATA_DIR}/taxi/yellow_trip_sample.csv"
ICEBERG_WAREHOUSE = f"{DATA_DIR}/warehouse"
PARQUET_SINK = f"{DATA_DIR}/warehouse/parquet"

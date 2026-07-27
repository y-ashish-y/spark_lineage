"""Generate the two demo notebooks as JSON (lightweight, predictable)."""

from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4


def _cell(cell_type: str, source: str) -> dict:
    return {
        "cell_type": cell_type,
        "id": uuid4().hex[:8],
        "metadata": {},
        "source": [line if line.endswith("\n") else line + "\n" for line in source.splitlines()],
    }


def _source(*lines: str) -> str:
    return "\n".join(lines)


def _markdown(*lines: str) -> dict:
    return _cell("markdown", _source(*lines))


def _code(*lines: str) -> dict:
    return _cell("code", _source(*lines))


def pyspark_notebook() -> dict:
    return {
        "cells": [
            _markdown(
                "# 01 — PySpark lineage with Spline",
                "",
                "Reads the 100-row NYC Yellow Taxi sample, transforms it via the",
                "DataFrame API, and writes both an Iceberg table and a Parquet sink.",
                "Each persistent write produces a Spline lineage event that you can",
                "inspect at <http://localhost:9090>.",
                "",
                "**Note**: Spline's stock Spark agent detects most write commands but",
                "the Iceberg `CreateTableAsSelect` flow wraps the plan in an",
                "Iceberg-specific node that the agent does not currently expose.",
                "So we also write a Parquet sink so the lineage is guaranteed to",
                "appear in Spline UI. The Iceberg table is still the artifact used",
                "for downstream queries.",
            ),
            _code(
                "from pyspark.sql import functions as F",
                "from _shared.spark_session import get_spark, SAMPLE_CSV, PARQUET_SINK",
                "",
                "spark = get_spark()",
                "spark.sparkContext.setLogLevel('WARN')",
                "print('Spark version:', spark.version)",
            ),
            _code(
                "taxi_pdf = spark.read.csv(SAMPLE_CSV, header=True, inferSchema=True)",
                "print('row count:', taxi_pdf.count())",
                "taxi_pdf.printSchema()",
                "taxi_pdf.show(5, truncate=False)",
            ),
            _markdown(
                "## Build the analytics layers",
                "",
                "Two derived frames:",
                "",
                "* `trip_durations` — adds a duration column and keeps numeric metrics.",
                "* `zone_revenue` — aggregates pickup-zone revenue.",
            ),
            _code(
                "trip_durations = (",
                "    taxi_pdf",
                "    .withColumn(",
                "        'trip_minutes',",
                "        (F.unix_timestamp('tpep_dropoff_datetime') - F.unix_timestamp('tpep_pickup_datetime')) / 60.0,",
                "    )",
                "    .select(",
                "        'PULocationID', 'DOLocationID', 'payment_type',",
                "        'trip_distance', 'trip_minutes', 'fare_amount', 'tip_amount', 'total_amount',",
                "    )",
                ")",
                "trip_durations.show(5, truncate=False)",
                "",
                "zone_revenue = (",
                "    taxi_pdf",
                "    .groupBy('PULocationID')",
                "    .agg(",
                "        F.count('*').alias('trips'),",
                "        F.round(F.sum('fare_amount'), 2).alias('revenue'),",
                "        F.round(F.avg('tip_amount'), 2).alias('avg_tip'),",
                "    )",
                "    .orderBy(F.desc('revenue'))",
                ")",
                "zone_revenue.show(5, truncate=False)",
            ),
            _markdown(
                "## Persist lineage",
                "",
                "Both writes are persistent actions, so Spline will emit a lineage",
                "event for each. The Iceberg table is the analytics-grade artifact,",
                "the Parquet sink guarantees Spline captures the column-level",
                "transformations.",
            ),
            _code(
                "(trip_durations.write",
                " .format('iceberg')",
                " .mode('overwrite')",
                " .saveAsTable('local.taxi.trip_durations'))",
                "",
                "# Spline-supported sink — always visible in Spline UI.",
                "(zone_revenue.write",
                " .mode('overwrite')",
                " .parquet(f'{PARQUET_SINK}/zone_revenue'))",
                "",
                "print('wrote trip_durations (iceberg) and zone_revenue (parquet)')",
            ),
            _markdown(
                "## Confirm what Spline sees",
                "",
                "The producer is on `spline-rest:8080`. After these writes, open",
                "<http://localhost:9090> and look for the latest execution events.",
                "The `zone_revenue` write should show a full column-level graph",
                "from `yellow_trip_sample.csv` to the Parquet sink.",
            ),
            _code(
                "import urllib.request, json",
                "events = json.loads(urllib.request.urlopen('http://spline-rest:8080/consumer/execution-events').read())",
                "events = events.get('items', events)",
                "print('captured events:', len(events))",
                "for e in events[:5]:",
                "    print(' -', e.get('name'), '|', e.get('id'))",
            ),
        ],
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "version": "3.11"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def spark_sql_notebook() -> dict:
    return {
        "cells": [
            _markdown(
                "# 02 — Spark SQL lineage with Spline",
                "",
                "Same NYC Yellow Taxi sample, queries expressed in Spark SQL.",
                "Each persistent `INSERT` produces a Spline lineage event.",
            ),
            _code(
                "from pyspark.sql import SparkSession",
                "from _shared.spark_session import get_spark, SAMPLE_CSV, PARQUET_SINK",
                "",
                "spark = get_spark()",
                "spark.sparkContext.setLogLevel('WARN')",
                "",
                "spark.sql('CREATE NAMESPACE IF NOT EXISTS local.taxi')",
                "spark.read.csv(SAMPLE_CSV, header=True, inferSchema=True).createOrReplaceTempView('taxi_raw')",
                "spark.sql('CREATE OR REPLACE TABLE local.taxi.taxi_raw USING iceberg AS SELECT * FROM taxi_raw')",
                "print('raw row count:', spark.sql('SELECT COUNT(*) FROM taxi_raw').first()[0])",
            ),
            _markdown(
                "## Aggregations",
                "",
                "Two SQL views we'll persist to Iceberg.",
            ),
            _code(
                "spark.sql(\"\"\"",
                "    CREATE OR REPLACE TEMP VIEW trip_durations_sql AS",
                "    SELECT",
                "        PULocationID,",
                "        DOLocationID,",
                "        payment_type,",
                "        trip_distance,",
                "        (UNIX_TIMESTAMP(tpep_dropoff_datetime) - UNIX_TIMESTAMP(tpep_pickup_datetime)) / 60.0 AS trip_minutes,",
                "        fare_amount,",
                "        tip_amount,",
                "        total_amount",
                "    FROM taxi_raw",
                "\"\"\")",
                "",
                "spark.sql(\"\"\"",
                "    CREATE OR REPLACE TEMP VIEW zone_revenue_sql AS",
                "    SELECT PULocationID,",
                "           COUNT(*) AS trips,",
                "           ROUND(SUM(fare_amount), 2) AS revenue,",
                "           ROUND(AVG(tip_amount), 2) AS avg_tip",
                "    FROM taxi_raw",
                "    GROUP BY PULocationID",
                "    ORDER BY revenue DESC",
                "\"\"\")",
                "",
                "spark.sql('SELECT * FROM trip_durations_sql LIMIT 5').show(truncate=False)",
                "spark.sql('SELECT * FROM zone_revenue_sql LIMIT 5').show(truncate=False)",
            ),
            _markdown(
                "## Persist (Spline captures every `INSERT INTO`)",
                "",
                "Both inserts are persistent actions, so Spline emits a lineage",
                "event for each. The Parquet insert is the enforced lineage sink.",
            ),
            _code(
                "spark.sql(\"\"\"",
                "    CREATE OR REPLACE TABLE local.taxi.trip_durations_sql USING iceberg AS",
                "    SELECT * FROM trip_durations_sql",
                "\"\"\")",
                "",
                "spark.sql(f\"\"\"",
                "    CREATE OR REPLACE TABLE local.taxi.zone_revenue_sql USING parquet AS",
                "    SELECT * FROM zone_revenue_sql",
                "\"\"\")",
                "",
                "print('SQL lineage produced for trip_durations_sql and zone_revenue_sql')",
            ),
            _markdown(
                "## Confirm",
                "",
                "Same idea as notebook 01 — list the events Spline recorded.",
            ),
            _code(
                "import urllib.request, json",
                "events = json.loads(urllib.request.urlopen('http://spline-rest:8080/consumer/execution-events').read())",
                "events = events.get('items', events)",
                "print('captured events:', len(events))",
                "for e in events[:5]:",
                "    print(' -', e.get('name'), '|', e.get('id'))",
            ),
        ],
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "version": "3.11"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def main() -> None:
    target_dir = Path(__file__).resolve().parents[1] / "notebooks"
    target_dir.mkdir(exist_ok=True)
    for name, payload in [
        ("01_pyspark_lineage.ipynb", pyspark_notebook()),
        ("02_spark_sql_lineage.ipynb", spark_sql_notebook()),
    ]:
        (target_dir / name).write_text(json.dumps(payload, indent=2))
        print("wrote", name)


if __name__ == "__main__":
    main()

"""Generate the two demo notebooks as JSON (lightweight, predictable)."""

from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

LINEAGE_CHEATSHEET = (
    "## Lineage cheat sheet",
    "",
    "| Your transformation | Spline node to click | Column to inspect |",
    "|--------------------|----------------------|-------------------|",
    "| `UNIX_TIMESTAMP` diff | `Project` | `trip_minutes` |",
    "| `GROUP BY` + `SUM` | `Aggregate` | `revenue`, `trips` |",
    "| Column select | `Project` | dropped columns absent in output |",
    "",
    "In Spline UI: Execution Plan → turn off Compact view → click the node →",
    "Output Schema → select a column → Lineage.",
)


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


def _lineage_inspect_cells(sinks: list[tuple[str, str]]) -> list[dict]:
    """Markdown + code cells for Consumer API column lineage inspection."""
    sink_lines = "\n".join(
        f"inspect_event_column('{name}', '{column}')" for name, column in sinks
    )
    return [
        _markdown(
            "## Inspect lineage (Consumer API)",
            "",
            "Text fallback when the graph UI feels opaque. Also open",
            "<http://localhost:9090> and follow the cheat sheet above.",
            "",
            "Consumer API docs: <http://localhost:8080/docs/consumer.html>",
        ),
        _code(
            "from _shared.lineage_inspect import list_recent_events, inspect_event_column",
            "",
            "list_recent_events()",
            sink_lines,
        ),
    ]


def pyspark_notebook() -> dict:
    return {
        "cells": [
            _markdown(
                "# 01 — PySpark lineage with Spline",
                "",
                "Reads the 100-row NYC Yellow Taxi sample, transforms it via the",
                "DataFrame API, and writes Parquet sinks so Spline captures full",
                "column-level lineage. Iceberg tables are optional analytics artifacts",
                "with weaker lineage visibility.",
                "",
                "Inspect results at <http://localhost:9090>.",
                "",
                *LINEAGE_CHEATSHEET,
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
                "## Persist lineage (Parquet — primary)",
                "",
                "Parquet writes produce the clearest Spline graphs: `Project` for",
                "`trip_minutes`, `Aggregate` for `revenue`. Inspect these events first",
                "in the UI.",
            ),
            _code(
                "(trip_durations.write",
                " .mode('overwrite')",
                " .parquet(f'{PARQUET_SINK}/trip_durations'))",
                "",
                "(zone_revenue.write",
                " .mode('overwrite')",
                " .parquet(f'{PARQUET_SINK}/zone_revenue'))",
                "",
                "print('wrote trip_durations and zone_revenue (parquet)')",
            ),
            _markdown(
                "## Optional: Iceberg tables (analytics artifact)",
                "",
                "Iceberg `CreateTableAsSelect` wraps the Spark plan in extra nodes.",
                "Use these tables for downstream SQL, but prefer the Parquet events",
                "above when exploring column lineage in Spline UI.",
            ),
            _code(
                "spark.sql('CREATE NAMESPACE IF NOT EXISTS local.taxi')",
                "",
                "(trip_durations.write",
                " .format('iceberg')",
                " .mode('overwrite')",
                " .saveAsTable('local.taxi.trip_durations'))",
                "",
                "(zone_revenue.write",
                " .format('iceberg')",
                " .mode('overwrite')",
                " .saveAsTable('local.taxi.zone_revenue'))",
                "",
                "print('wrote trip_durations and zone_revenue (iceberg)')",
            ),
            *_lineage_inspect_cells(
                [
                    ("trip_durations", "trip_minutes"),
                    ("zone_revenue", "revenue"),
                ]
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
                "Each persisted `CREATE TABLE AS` produces a Spline lineage event.",
                "Transformations are inlined in the write — not split across temp views.",
                "",
                "Inspect results at <http://localhost:9090>.",
                "",
                *LINEAGE_CHEATSHEET,
            ),
            _code(
                "from _shared.spark_session import get_spark, SAMPLE_CSV, PARQUET_SINK",
                "",
                "spark = get_spark()",
                "spark.sparkContext.setLogLevel('WARN')",
                "",
                "spark.read.csv(SAMPLE_CSV, header=True, inferSchema=True).createOrReplaceTempView('taxi_raw')",
                "print('raw row count:', spark.sql('SELECT COUNT(*) FROM taxi_raw').first()[0])",
            ),
            _markdown(
                "## Persist lineage (full SQL in each write)",
                "",
                "Both statements contain the full transformation and write Parquet tables.",
                "Spline should show `Project` (trip duration calc) and `Aggregate`",
                "(zone revenue) directly on these execution plans.",
            ),
            _code(
                "spark.sql(\"\"\"",
                "    CREATE OR REPLACE TABLE local.taxi.trip_durations_sql USING parquet AS",
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
                "    CREATE OR REPLACE TABLE local.taxi.zone_revenue_sql USING parquet AS",
                "    SELECT PULocationID,",
                "           COUNT(*) AS trips,",
                "           ROUND(SUM(fare_amount), 2) AS revenue,",
                "           ROUND(AVG(tip_amount), 2) AS avg_tip",
                "    FROM taxi_raw",
                "    GROUP BY PULocationID",
                "    ORDER BY revenue DESC",
                "\"\"\")",
                "",
                "print('wrote trip_durations_sql and zone_revenue_sql (parquet)')",
            ),
            *_lineage_inspect_cells(
                [
                    ("trip_durations_sql", "trip_minutes"),
                    ("zone_revenue_sql", "revenue"),
                ]
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

# Spark Lineage with Spline, Iceberg & PySpark

Minimal, reproducible demo that captures Apache Spark data lineage with
[AbsaOSS Spline](https://absaoss.github.io/spline/) while walking through
a 100-row sample of the public NYC Yellow Taxi trip dataset using
Apache Iceberg as the table format.

The whole stack runs via Docker Compose:

| Service          | Port  | Purpose                                                |
|------------------|-------|--------------------------------------------------------|
| `jupyter`        | 8888  | PySpark / Spark SQL notebooks with Spline pre-wired   |
| `spline-rest`    | 8080  | Spline REST server (Producer + Consumer API)           |
| `spline-ui`      | 9090  | Spline Web UI for visualizing lineage                  |
| `arangodb`       | 8529  | Spline metadata store                                  |
| `db-init`        | -     | One-shot Spline database initializer                   |

## Quick start

```bash
docker compose up -d
docker compose logs -f jupyter
# Wait for the token URL, then open it in your browser.
```

Run the two notebooks in order:

1. `notebooks/01_pyspark_lineage.ipynb` — DataFrame API
2. `notebooks/02_spark_sql_lineage.ipynb` — Spark SQL

Then open Spline UI at <http://localhost:9090> to inspect captured lineage.

## Reading lineage in Spline UI

Spline exposes two levels of detail:

| View | What it shows |
|------|---------------|
| **Execution Event → Overview** | High-level flow: source file → Spark job → output table |
| **Execution Plan → Overview** | Spark logical plan nodes (`Project`, `Aggregate`, `View`, …) |

Column-level detail is available but not on the default overview. To inspect
transformations:

1. Open **Execution Plans** (or drill into a plan from an event).
2. Turn off **Compact view**.
3. Click a node such as `Project` or `Aggregate`.
4. In the right panel, open **Output Schema** and click a column (e.g.
   `trip_minutes`, `revenue`).
5. Click **Lineage** / **Details** to see upstream columns and expressions.

**Which writes to inspect** (after running the notebooks):

| Output | Transformation | Node to click | Column |
|--------|----------------|---------------|--------|
| `zone_revenue` / `zone_revenue_sql` | `GROUP BY` + `SUM` | `Aggregate` | `revenue` |
| `trip_durations` / `trip_durations_sql` | datetime diff | `Project` | `trip_minutes` |

The notebooks also include a **Consumer API** cell that prints the same
upstream columns as text. API docs:
<http://localhost:8080/docs/consumer.html>

**Parquet vs Iceberg:** Parquet writes produce the clearest Spline graphs.
Iceberg `CreateTableAsSelect` adds wrapper nodes that hide column lineage.
Notebook 01 writes Parquet first (primary lineage examples) and Iceberg
second (optional analytics tables).

**What Spline does not track:** structural lineage only — which columns derive
from which sources and through which operations. Spline does **not** capture
runtime row values or “what data changed”. For row-level diffs, use data
quality or snapshot comparison tools.

## Validation

```bash
docker compose exec -T jupyter python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
print('pyspark', spark.version)
print('taxi rows:', spark.read.csv('/home/jovyan/data/taxi/yellow_trip_sample.csv', header=True).count())
"
```

## Notes

- Spark 3.5 / Scala 2.12, Iceberg 1.10.1, Spline agent 2.3.0, Spline server 1.0.0-RC3.
- The taxi dataset is a 100-row deterministic subset of the public NYC TLC
  Yellow Taxi parquet release (see `data/taxi/README.md`).
- Regenerate notebooks after editing the generator:
  `python scripts/build_notebooks.py`

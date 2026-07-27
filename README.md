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

## Validation

```bash
docker compose exec -T jupyter python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
print('pyspark', spark.version)
print('taxi rows:', spark.read.parquet('/home/jovyan/data/warehouse/taxi/yellow_trip_sample').count())
"
```

## Notes

- Spark 3.5 / Scala 2.12, Iceberg 1.10.1, Spline agent 2.3.0, Spline server 1.0.0-RC3.
- The taxi dataset is a 100-row deterministic subset of the public NYC TLC
  Yellow Taxi parquet release (see `data/taxi/README.md`).
- Stock Spline supports the most common Spark providers. Iceberg `CreateTableAsSelect`
  adds an extra wrapper node; the demo writes both an Iceberg table (for the
  data engineering flow) and a Parquet sink (for guaranteed Spline lineage).
  See `notebooks/01_pyspark_lineage.ipynb` for the explanation.

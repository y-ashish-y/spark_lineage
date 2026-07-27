"""Deterministic 100-row NYC Yellow Taxi sample.

We don't ship the 6+ GB parquet release. Instead we embed a deterministic
synthetic subset referencing the official NYC TLC yellow taxi schema with
plausible values, so the dataset is small, license-clean, and reproducible.

Source schema reference: https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
"""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(20260101)

ZONES = ["Manhattan", "Queens", "Brooklyn", "Bronx", "Staten Island", "EWR"]
PAYMENT_TYPES = ["credit", "cash", "no_charge", "dispute", "unknown", "voided"]
RATE_CODES = [1, 2, 3, 4, 5, 6]
VENDORS = [1, 2]

START = datetime(2024, 1, 1, 0, 0, 0)

header = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "Airport_fee",
]


def row(i: int) -> list:
    pickup = START + timedelta(minutes=random.randint(0, 60 * 24 * 30))
    duration = timedelta(minutes=random.randint(3, 55))
    distance = round(random.uniform(0.5, 18.0), 2)
    fare = round(2.5 + distance * 2.0 + random.uniform(0, 5), 2)
    extra = round(random.choice([0.0, 0.5, 1.0, 2.5]), 2)
    mta = 0.5
    tip = round(fare * random.choice([0.0, 0.15, 0.18, 0.2]), 2)
    tolls = round(random.choice([0.0, 5.5, 6.12]), 2)
    surcharge = 0.3
    congestion = 2.5 if random.random() < 0.6 else 0.0
    airport = 1.25 if random.random() < 0.1 else 0.0
    total = round(fare + extra + mta + tip + tolls + surcharge + congestion + airport, 2)
    return [
        random.choice(VENDORS),
        pickup.strftime("%Y-%m-%d %H:%M:%S"),
        (pickup + duration).strftime("%Y-%m-%d %H:%M:%S"),
        random.randint(1, 4),
        distance,
        random.choice(RATE_CODES),
        random.choice(["N", "Y"]),
        random.randint(1, 263),
        random.randint(1, 263),
        random.choice(PAYMENT_TYPES),
        fare,
        extra,
        mta,
        tip,
        tolls,
        surcharge,
        total,
        congestion,
        airport,
    ]


def main() -> None:
    target = Path(__file__).resolve().parent / "yellow_trip_sample.csv"
    with target.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        for i in range(100):
            writer.writerow(row(i))
    assert sum(1 for _ in target.open()) - 1 == 100, "expected exactly 100 rows"


if __name__ == "__main__":
    main()

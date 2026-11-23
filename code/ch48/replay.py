"""Replay a morning of NYC taxi trips as a stream of small JSON Lines files.

A trip record reaches the TLC when the trip ends, so each file holds the trips that
*ended* (dropoff) in one slice of time. The event time we care about is the pickup.
That is real lateness, not a simulation: a 40-minute trip arrives 40 minutes after its pickup.
"""
import datetime as dt
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pyarrow.compute as pc
import pyarrow.parquet as pq
from common.book import TRIPS

DAY = dt.datetime(2026, 6, 10)           # a Wednesday in June 2026
SCHEMA = ("trip_id STRING, pickup_ts TIMESTAMP, dropoff_ts TIMESTAMP, "
          "PULocationID INT, DOLocationID INT, trip_distance DOUBLE, "
          "fare_amount DOUBLE, total_amount DOUBLE")


def trips_between(start, end):
    """Trips whose dropoff is in [start, end), sorted by dropoff, as dicts."""
    cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID",
            "trip_distance", "fare_amount", "total_amount"]
    t = pq.read_table(TRIPS, columns=cols)
    t = t.filter(pc.and_(pc.greater_equal(t["tpep_dropoff_datetime"], start),
                         pc.less(t["tpep_dropoff_datetime"], end)))
    t = t.sort_by([("tpep_dropoff_datetime", "ascending"), ("tpep_pickup_datetime", "ascending"),
                   ("PULocationID", "ascending")])
    rows = []
    for i, r in enumerate(t.to_pylist()):
        rows.append({"trip_id": f"{start:%m%d}-{i:05d}",
                     "pickup_ts": r["tpep_pickup_datetime"].isoformat(),
                     "dropoff_ts": r["tpep_dropoff_datetime"].isoformat(),
                     "PULocationID": r["PULocationID"], "DOLocationID": r["DOLocationID"],
                     "trip_distance": r["trip_distance"], "fare_amount": r["fare_amount"],
                     "total_amount": r["total_amount"]})
    return rows


def slices(start_hour=8, minutes=5, count=12):
    """[(file name, JSON Lines text, number of trips)], one file per `minutes` of dropoffs."""
    start = DAY + dt.timedelta(hours=start_hour)
    rows = trips_between(start, start + dt.timedelta(minutes=minutes * count))
    out = []
    for k in range(count):
        lo = start + dt.timedelta(minutes=minutes * k)
        hi = lo + dt.timedelta(minutes=minutes)
        part = [r for r in rows if lo.isoformat() <= r["dropoff_ts"] < hi.isoformat()]
        out.append((f"trips_{lo:%H%M}.jsonl", "".join(json.dumps(r) + "\n" for r in part), len(part)))
    return out


if __name__ == "__main__":
    for name, _, n in slices():
        print(f"{name}  {n:>4} trips")

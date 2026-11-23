"""Chapter 56: copy days of the June 2026 taxi month into a pipeline project, one Parquet file per day.

  python make_days.py <project>/data/trips 2026-06-01 [2026-06-02 ...]
Reads $BOOK_DATA/nyc-taxi/yellow/yellow_tripdata_2026-06.parquet (Chapter 6 downloads it) and
writes trips_<day>.parquet with the trips that start on that day. No Spark is needed.
"""
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq

data = Path(os.environ.get("BOOK_DATA", Path.home() / "spark-masterclass" / "data"))
month = pq.read_table(data / "nyc-taxi" / "yellow" / "yellow_tripdata_2026-06.parquet")
out = Path(sys.argv[1])
out.mkdir(parents=True, exist_ok=True)
for day in sys.argv[2:]:
    start = datetime.fromisoformat(day)
    end = start + timedelta(days=1)
    ts = month["tpep_pickup_datetime"]
    rows = month.filter(pc.and_(pc.greater_equal(ts, start), pc.less(ts, end)))
    pq.write_table(rows, out / f"trips_{day}.parquet")
    print(f"trips_{day}.parquet: {rows.num_rows:,} trips")

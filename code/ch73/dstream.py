"""Chapter 73: the first streaming API, DStreams, still in the 4.3 package and deprecated.

A queueStream of three RDDs, one RDD for each one-second batch, counts pickups for each zone.
Classic only. Run from code/:  uv run python ch73/dstream.py
"""
import sys
import time
import warnings
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import session

spark = session("ch73-dstream", spark_ui_showConsoleProgress="false")
sc = spark.sparkContext

from pyspark.streaming import StreamingContext
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    ssc = StreamingContext(sc, batchDuration=1)
for w in caught:
    print(f"{w.category.__name__}: {w.message}")

batches = [sc.parallelize([161, 237, 161, 236]), sc.parallelize([237, 237]), sc.parallelize([161])]
counts = ssc.queueStream(batches).map(lambda z: (z, 1)).reduceByKey(lambda a, b: a + b)
counts.foreachRDD(lambda t, rdd: print(t.strftime("%H:%M:%S"), sorted(rdd.collect())))
ssc.start()
time.sleep(3.5)
ssc.stop(stopSparkContext=False, stopGraceFully=False)

"""Chapter 9: DataFrame.plot (PySpark 4.x, plotly backend). What each kind of plot sends to the driver.

Works on Classic and on Connect (set SPARK_REMOTE). Needs plotly (kaleido only to write the SVG figures):
  uv run --with plotly==6.5.0 --with kaleido==1.2.0 python ch09/plot.py
"""
import os
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, ROOT, TRIPS, ZONES

spark = session("ch09-plot", spark_ui_showConsoleProgress="false")
remote = bool(os.environ.get("SPARK_REMOTE"))
trips = spark.read.parquet(str(TRIPS))
print("session:", type(spark).__module__)
print("spark.sql.pyspark.plotting.max_rows =", spark.conf.get("spark.sql.pyspark.plotting.max_rows"))

print("\n== 1. bar: a top-n plot. Aggregate first, then plot the small result")
zones = spark.read.option("header", True).csv(str(ZONES))
top = (trips.groupBy("PULocationID").count()
       .join(zones, trips.PULocationID == zones.LocationID).orderBy(F.desc("count")).limit(10)
       .select("Zone", "count"))
fig = top.plot.bar(x="Zone", y="count")
print("type:", type(fig).__module__ + "." + type(fig).__name__, " bars:", len(fig.data[0].x))
print("first bar:", fig.data[0].x[0], fig.data[0].y[0])

print("\n== 2. bar on the raw 3.8 million rows: only the first max_rows rows reach the plot")
raw = trips.select(F.col("PULocationID").cast("string").alias("zone"), "fare_amount").plot.bar(x="zone", y="fare_amount")
print("bars in the figure:", len(raw.data[0].x))

print("\n== 3. scatter: a sampled plot")
sc = trips.where("trip_distance BETWEEN 0.1 AND 30 AND fare_amount BETWEEN 1 AND 150") \
          .plot.scatter(x="trip_distance", y="fare_amount")
print("points in the figure:", len(sc.data[0].x), "(a random sample of the filtered rows)")

print("\n== 4. hist, box and kde: Spark computes the numbers, the driver gets only the result")
h = trips.where("fare_amount BETWEEN 0 AND 100").select("fare_amount", "tip_amount").plot.hist(bins=20)
print("hist traces:", [t.name for t in h.data], " bins in each:", len(h.data[0].y))
print("hist bar heights (fare_amount, first 5):", [int(v) for v in h.data[0].y[:5]])
b = trips.where("fare_amount BETWEEN 0 AND 200").plot.box(column="fare_amount")
bx = b.data[0]
print(f"box: q1={bx.q1[0]} median={bx.median[0]} q3={bx.q3[0]} lowerfence={bx.lowerfence[0]} upperfence={bx.upperfence[0]}")
k = trips.where("fare_amount BETWEEN 0 AND 100").plot.kde(column="fare_amount", bw_method=0.3, ind=50)
print("kde points:", len(k.data[0].x))

print("\n== 5. an unsupported backend")
try:
    top.plot(kind="bar", backend="matplotlib", x="Zone", y="count")
except Exception as e:
    print(type(e).__name__, e.getCondition(), "|", str(e).split("] ", 1)[1][:70])

if not remote:
    mid, out = "#8c959f", ROOT / "book" / "media" / "figures"   # mid-tone text: reads on light and dark pages

    def style(f, title, w=760, h=380):
        f.update_layout(width=w, height=h, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                        font=dict(color=mid, size=13), title=title, margin=dict(l=60, r=20, t=50, b=60))
        f.update_xaxes(gridcolor="rgba(140,149,159,0.25)", zerolinecolor=mid); f.update_yaxes(gridcolor="rgba(140,149,159,0.25)", zerolinecolor=mid)
        return f

    fig.update_traces(marker_color="#e8590c")
    style(fig, "top.plot.bar(x='Zone', y='count'): trips by pickup zone, top 10", h=420).write_image(str(out / "ch09-plot-bar.svg"))
    sc.update_traces(marker=dict(color="#3b82f6", size=4, opacity=0.5))
    style(sc, f"plot.scatter: {len(sc.data[0].x)} sampled trips, trip_distance against fare_amount") \
        .write_image(str(out / "ch09-plot-scatter.svg"))
    print("\nwrote ch09-plot-bar.svg and ch09-plot-scatter.svg")

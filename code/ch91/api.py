"""Chapter 91: options, interop with Spark and pandas, functions with type hints, plotting and limits.

Works on Classic and on Connect (set SPARK_REMOTE). Plotting needs plotly and kaleido:
  uv run --with plotly==6.5.0 --with kaleido==1.2.0 python ch91/api.py
  SPARK_REMOTE=sc://localhost:15117 uv run --with plotly==6.5.0 --with kaleido==1.2.0 python ch91/api.py
"""
import os
import sys
import time
import warnings
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from common.book import session, plan, ROOT, TRIPS

spark = session("ch91-api")
import pyspark.pandas as ps                                      # noqa: E402

warnings.simplefilter("ignore")
remote = bool(os.environ.get("SPARK_REMOTE"))
print("session:", type(spark).__module__)
COLS = ["PULocationID", "payment_type", "fare_amount", "tip_amount", "trip_distance"]
psdf = ps.read_parquet(str(TRIPS), columns=COLS)


def attempt(label, fn):
    try:
        out = fn()
        print(f"  {label:40} ok: {out}")
    except Exception as e:
        cond = e.getCondition() if hasattr(e, "getCondition") else ""
        print(f"  {label:40} {type(e).__name__} {cond}")
        if not cond:
            print(f"  {'':40} {str(e).splitlines()[0][:48]}")


print("\n== 1. options")
for k in ("compute.default_index_type", "compute.ops_on_diff_frames", "compute.shortcut_limit",
          "compute.max_rows", "compute.ansi_mode_support", "display.max_rows", "plotting.backend"):
    print(f"  {k:28} {ps.get_option(k)}")
with ps.option_context("display.max_rows", 3):
    print("  inside option_context('display.max_rows', 3):", ps.get_option("display.max_rows"))

print("\n== 2. interop: to_spark, pandas_api, to_pandas, from_pandas, ps.sql")
sdf = psdf.to_spark()
print("  to_spark() columns:            ", sdf.columns)
sdf_idx = psdf.to_spark(index_col="row")
print("  to_spark(index_col='row'):     ", sdf_idx.columns[:2], "...")
back = sdf_idx.pandas_api(index_col="row")
print("  pandas_api(index_col='row'):   ", type(back).__name__, back.index.name)
print("  pandas_api() with no index:    ", [l.strip()[:44] for l in plan(sdf.pandas_api().spark.frame(index_col="i"), "simple")
                                            .splitlines() if "Attach" in l or "Window" in l][0])
small = psdf[psdf.PULocationID == 132].head(1000).to_pandas()
print("  to_pandas():                   ", type(small).__module__ + "." + type(small).__name__, small.shape)
print("  ps.from_pandas(pdf):           ", type(ps.from_pandas(small)).__name__)
top = ps.sql("SELECT PULocationID, count(*) AS n FROM {t} GROUP BY 1 ORDER BY n DESC LIMIT 3", t=psdf)
print("  ps.sql(... {t} ...):           ", top.to_pandas().values.tolist())

print("\n== 3. functions: apply_batch and transform_batch, with and without type hints")
calls = {"n": 0}


def tip_pct(pdf):                                              # no hints
    return pdf.assign(pct=pdf.tip_amount / pdf.fare_amount.where(pdf.fare_amount > 0))


def tip_pct_hinted(pdf) -> ps.DataFrame[COLS[0]: int, "pct": float]:   # noqa: F821
    return pd.DataFrame({"PULocationID": pdf.PULocationID,
                         "pct": pdf.tip_amount / pdf.fare_amount.where(pdf.fare_amount > 0)})


for _ in range(2):                                             # the second round: Python workers already up
    t = time.perf_counter(); r1 = psdf.pandas_on_spark.apply_batch(tip_pct); d1 = time.perf_counter() - t
    t = time.perf_counter(); r2 = psdf.pandas_on_spark.apply_batch(tip_pct_hinted); d2 = time.perf_counter() - t
print(f"  no return hint:   defined in {d1:.2f} s (runs the function on a sample to infer the schema)")
print(f"  with return hint: defined in {d2:.2f} s, schema {r2.spark.frame().schema.simpleString()[:60]}")
print("  mean pct, both:", round(r1["pct"].mean(), 4), round(r2["pct"].mean(), 4))
print("  plan of the hinted version:", [l.strip()[:60] for l in plan(r2.spark.frame(), "simple").splitlines()
                                       if "ArrowEval" in l or "MapInPandas" in l or "MapInArrow" in l][:1])

print("\n== 4. operations on two different DataFrames")
a = ps.read_parquet(str(TRIPS), columns=["fare_amount"]).head(5)
b = ps.read_parquet(str(TRIPS), columns=["tip_amount"]).head(5)
s = a["fare_amount"] + b["tip_amount"]
print("  a.fare_amount + b.tip_amount: ok, the plan joins on the index:",
      "Join" in plan(s.to_frame().spark.frame(), "simple"))
with ps.option_context("compute.ops_on_diff_frames", False):
    attempt("same with ops_on_diff_frames=False", lambda: (a["fare_amount"] + b["tip_amount"]).to_pandas())

print("\n== 5. limits")
attempt("psdf.append (removed in 4.0)", lambda: psdf.append)
attempt("psdf.iteritems (removed in 4.0)", lambda: psdf.iteritems)
attempt("ps.Int64Index (removed in 4.0)", lambda: ps.Int64Index)
attempt("psdf.values (collects to the driver)", lambda: type(psdf.head(3).values).__name__)
attempt("psdf.iloc[5] (row by position)", lambda: psdf.iloc[5].round(2).tolist())
attempt("psdf.iloc[:3] (a slice is fine)", lambda: psdf.iloc[:3].shape)
attempt("psdf.fare_amount.rolling(3).mean()", lambda: psdf.fare_amount.head(5).rolling(3).mean().to_pandas().tolist())

print("\n== 6. plotting (plotly)")
fig = psdf["fare_amount"][(psdf.fare_amount > 0) & (psdf.fare_amount < 100)].plot.hist(bins=40, title="fare_amount")
print("  hist type:", type(fig).__module__ + "." + type(fig).__name__, " bars:", len(fig.data[0].x))
by_zone = psdf.groupby("PULocationID")["fare_amount"].count().nlargest(8)
fig2 = by_zone.sort_values().plot.barh(title="trips per pickup zone, top 8")
print("  barh rows:", len(fig2.data[0].y))
flat = ps.read_parquet(str(TRIPS), columns=["RatecodeID", "fare_amount"])
jfk = flat[flat.RatecodeID == 2]["fare_amount"]
print(f"  RatecodeID 2 (JFK) trips: {len(jfk):,}, most common fare: {jfk.mode().to_pandas().tolist()}")
if not remote:
    mid = "#8c959f"                                               # mid-tone: reads on light and dark pages
    fig.update_traces(marker_color="#e8590c", text=None, texttemplate=None, hovertemplate=None)
    fig.update_layout(width=760, height=360, showlegend=False, paper_bgcolor="rgba(0,0,0,0)",
                      plot_bgcolor="rgba(0,0,0,0)", font=dict(color=mid, size=13), margin=dict(l=60, r=20, t=50, b=50),
                      title="fare_amount, 0 to 100 (psdf.plot.hist, 40 bins)", xaxis_title="fare_amount",
                      yaxis_title="trips")
    fig.update_xaxes(gridcolor="rgba(140,149,159,0.25)"); fig.update_yaxes(gridcolor="rgba(140,149,159,0.25)")
    fig.write_image(str(ROOT / "book" / "media" / "figures" / "ch91-fare-hist.svg"))
    print("  wrote book/media/figures/ch91-fare-hist.svg")

"""Chapter 19: approximate aggregates against exact ones: results, errors, plans and shuffle sizes.
Run from code/:  uv run python ch19/approx.py      (timings: ch19/bench.py)
"""
from aggbook import start, nodes, sql_metrics

spark = start("ch19-approx", spark_driver_memory="4g")


def shuffle(spark):
    out = []
    for nm, _, m in sql_metrics(spark, ("shuffle records written",)):
        if nm == "Exchange" and m:
            out.append(int(m["shuffle records written"].replace(",", "")))
    return out


# 1. Distinct counts: exact, HyperLogLog++ at two error targets, and a DataSketches HLL sketch.
print("== 1. distinct pickup times for each borough: exact against approximate")
# Two queries: a DISTINCT aggregate next to object aggregates makes the partial step group by
# (Borough, value), and that key set is large. Join the results on Borough instead.
Q = """SELECT e.Borough, e.exact, a.hllpp_5pct, a.hllpp_1pct, a.ds_hll FROM
       (SELECT Borough, count(DISTINCT tpep_pickup_datetime) AS exact FROM june GROUP BY Borough) e JOIN
       (SELECT Borough,
          approx_count_distinct(tpep_pickup_datetime)            AS hllpp_5pct,
          approx_count_distinct(tpep_pickup_datetime, 0.01)      AS hllpp_1pct,
          hll_sketch_estimate(hll_sketch_agg(CAST(tpep_pickup_datetime AS STRING))) AS ds_hll
       FROM june GROUP BY Borough) a ON e.Borough = a.Borough
       WHERE e.Borough IN ('Manhattan', 'Queens', 'Brooklyn', 'Bronx') ORDER BY exact DESC"""
rows = spark.sql(Q).collect()
print(f"  {'Borough':<10}{'exact':>10}{'hll++ 5%':>11}{'err':>7}{'hll++ 1%':>11}{'err':>7}{'ds hll':>10}{'err':>7}")
for r in rows:
    e = lambda v: f"{100 * (v - r.exact) / r.exact:+.2f}%"
    print(f"  {r.Borough:<10}{r.exact:>10,}{r.hllpp_5pct:>11,}{e(r.hllpp_5pct):>7}{r.hllpp_1pct:>11,}"
          f"{e(r.hllpp_1pct):>7}{r.ds_hll:>10,}{e(r.ds_hll):>7}")

# 2. The plans: the exact count shuffles every distinct (key, value) pair; the sketch shuffles one buffer.
print("\n== 2. what each one shuffles")
for label, agg in (("count(DISTINCT)", "count(DISTINCT tpep_pickup_datetime)"),
                   ("approx_count_distinct", "approx_count_distinct(tpep_pickup_datetime)")):
    df = spark.sql(f"SELECT Borough, {agg} AS n FROM june GROUP BY Borough")
    df.write.format("noop").mode("overwrite").save()
    aggs = [l.strip().lstrip("+- ").split("(")[0] for l in nodes(df) if "Aggregate(" in l]
    print(f"  {label:<23} aggregates: {len(aggs)}  shuffle records written: {shuffle(spark)}")

# 3. Percentiles: exact percentile against percentile_approx at two accuracies.
print("\n== 3. the median fare for each borough")
P = """SELECT Borough,
          percentile(fare_amount, 0.5)                AS exact,
          percentile_approx(fare_amount, 0.5, 100)    AS approx_100,
          percentile_approx(fare_amount, 0.5)         AS approx_10000,
          kll_sketch_get_quantile_double(kll_sketch_agg_double(fare_amount), 0.5) AS kll
       FROM june WHERE Borough IN ('Manhattan', 'Queens', 'Brooklyn', 'Bronx') GROUP BY Borough ORDER BY Borough"""
spark.sql(P).show()
for agg in ("percentile(fare_amount, 0.5)", "percentile_approx(fare_amount, 0.5)", "median(fare_amount)"):
    df = spark.sql(f"SELECT Borough, {agg} FROM june GROUP BY Borough")
    ops = [l.strip().lstrip("+- ").split("(")[0] for l in nodes(df) if "Aggregate(" in l]
    print(f"  {agg:<38} -> {ops[0]}")

"""Chapter 32: adaptive query execution, one query per feature, with the plans before and after.

Classic only for the rule tracker (df._jdf). Run from code/:  uv run python ch32/aqe.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, plan, ui_api, TRIPS, ZONES

spark = session("ch32-aqe", spark_sql_session_timeZone="UTC")
trips = spark.read.parquet(str(TRIPS))

def section(t):
    print(f"\n######## {t}\n")


def short(text, width=118, keep=lambda l: True):
    return "\n".join((l if len(l) <= width else l[:width] + "...") for l in text.splitlines() if keep(l))


def run(df, label):
    spark.sparkContext.setJobDescription(label)
    rows = df.collect()
    jobs = [j for j in ui_api("jobs") if j.get("description") == label]
    stages = sorted({s for j in jobs for s in j["stageIds"]})
    ran = []
    for sid in stages:
        for st in ui_api(f"stages/{sid}"):
            if st["status"] == "COMPLETE":
                ran.append(f"stage {sid}: {st['numTasks']} tasks")
    print(f"{len(rows)} rows; {len(jobs)} jobs; " + ", ".join(ran))
    return rows

WANT = set(sys.argv[1:]) or {"coalesce", "join", "skew", "actions", "settings"}   # e.g. "join skew"


def want(name):
    return not WANT or name in WANT



# 1. Coalescing: 200 planned shuffle partitions, read back as a few.
if want("coalesce"):
    section("1. coalesce: trips per pickup zone")
    by_zone = trips.groupBy("PULocationID").agg(F.count("*").alias("n"))
    run(by_zone, "aqe coalesce")
    print(short(plan(by_zone, "simple"), keep=lambda l: "Initial" in l or "Final" in l or "AQEShuffleRead" in l
                or "QueryStage" in l or "Exchange" in l))

# 2. A sort-merge join that becomes a broadcast join once AQE sees the real size.
if want("join"):
    section("2. join: sort-merge planned, broadcast at run time")
    big_fares = (trips.where(F.col("fare_amount") > 500)
                 .select("PULocationID", "fare_amount", "tip_amount", "trip_distance", "tpep_pickup_datetime"))
    all_trips = trips.select("PULocationID", "DOLocationID", "total_amount", "tolls_amount",
                             F.col("tpep_dropoff_datetime").alias("dropoff"))
    def joined_by(key):
        return (all_trips.join(big_fares, "PULocationID")
                .groupBy(key)
                .agg(F.count("*").alias("n"), F.max("fare_amount").alias("max_fare"), F.max("tip_amount"),
                     F.max("trip_distance"), F.max("tpep_pickup_datetime"), F.sum("total_amount"),
                     F.max("tolls_amount"), F.max("dropoff")))


    for name, side in (("left", all_trips), ("right", big_fares)):
        print(f"estimated size of the {name} side:",
              [l.split("Statistics(")[1].rstrip(")") for l in plan(side, "cost").splitlines()
               if "Project" in l and "Statistics(" in l][0])
    for key, ratio in (("PULocationID", "0.2"), ("DOLocationID", "0.2"), ("DOLocationID", "0.0"), ("PULocationID", "0.0")):
        spark.conf.set("spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin", ratio)
        joined = joined_by(key)
        print(f"\n-- grouped by {key}, nonEmptyPartitionRatioForBroadcastJoin={ratio}; before the run:")
        print(short(plan(joined, "simple"), keep=lambda l: "Join" in l))
        run(joined, f"aqe join {key} {ratio}")
        print("after the run:")
        print(short(plan(joined, "simple"), keep=lambda l: "Join" in l or "AQEShuffleRead" in l
                    or "Final Plan" in l or "BroadcastQueryStage" in l or "Exchange" in l))

    spark.conf.unset("spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin")
    right_rows = big_fares.select("PULocationID").collect()
    print(f"\nthe right side at run time: {len(right_rows)} rows in {len(set(r[0] for r in right_rows))} pickup zones,"
          f" so at most {len(set(r[0] for r in right_rows))} of 200 shuffle partitions hold data")

# 2b. The same join in a short form, for the film: one line per setting.
if want("joinbrief"):
    big_fares = (trips.where(F.col("fare_amount") > 500)
                 .select("PULocationID", "fare_amount", "tip_amount", "trip_distance", "tpep_pickup_datetime"))
    all_trips = trips.select("PULocationID", "DOLocationID", "total_amount", "tolls_amount",
                             F.col("tpep_dropoff_datetime").alias("dropoff"))
    rows = big_fares.select("PULocationID").collect()
    zones_hit = len({r[0] for r in rows})
    print(f"right side at run time: {len(rows)} rows, {zones_hit} zones: {zones_hit} of 200 partitions hold data")
    for ratio in ("0.2", "0.0"):
        spark.conf.set("spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin", ratio)
        q = all_trips.join(big_fares, "PULocationID").groupBy("DOLocationID").agg(
            F.count("*"), F.max("fare_amount"), F.max("tip_amount"), F.max("trip_distance"),
            F.max("tpep_pickup_datetime"), F.sum("total_amount"), F.max("tolls_amount"), F.max("dropoff"))
        join_of = lambda t: next(w for w in ("BroadcastHashJoin", "SortMergeJoin") if w in t)
        before = join_of(plan(q, "simple"))
        label = f"aqe joinbrief {ratio}"
        spark.sparkContext.setJobDescription(label)
        q.collect()
        jobs = len([j for j in ui_api("jobs") if j.get("description") == label])
        after = plan(q, "simple").split("== Initial Plan ==")[0]
        local = ", AQEShuffleRead local" if "AQEShuffleRead local" in after else ""
        print(f"ratio {ratio}: planned {before}, ran {join_of(after)}, {jobs} jobs{local}")
    spark.conf.unset("spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin")

# 3. Skew: pickup zones are very uneven, so force a sort-merge join and let AQE split the big ones.
if want("skew"):
    section("3. skew: a sort-merge join on pickup zone, broadcast turned off")
    sizes = {k: spark.conf.get(k) for k in ("spark.sql.autoBroadcastJoinThreshold",
                                             "spark.sql.adaptive.advisoryPartitionSizeInBytes",
                                             "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes")}
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "256KB")
    spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "512KB")
    zones = spark.read.option("header", True).option("inferSchema", True).csv(str(ZONES))
    skewed = trips.select("PULocationID", "fare_amount").join(zones, trips.PULocationID == zones.LocationID)
    spark.sparkContext.setJobDescription("aqe skew")
    print("rows:", skewed.toArrow().num_rows)
    print(short(plan(skewed, "simple"), keep=lambda l: "Join" in l or "AQEShuffleRead" in l or "== " in l))
    for st in sorted(ui_api("stages?status=complete"), key=lambda s: s["stageId"]):
        if st.get("description") == "aqe skew":
            print(f"  stage {st['stageId']}: {st['numTasks']:>3} tasks, shuffle write {st['shuffleWriteBytes']:>11,} B, "
                  f"shuffle read {st['shuffleReadBytes']:>11,} B")
    # The biggest pickup zones, which made the skewed partitions.
    top = trips.groupBy("PULocationID").count().orderBy(F.desc("count")).limit(3).collect()
    med = trips.groupBy("PULocationID").count().agg(F.median("count")).first()[0]
    print("  largest zones:", ", ".join(f"{r[0]}: {r[1]:,} trips" for r in top), f"(median zone: {med:,.0f})")
    for k, v in sizes.items():
        spark.conf.set(k, v)

# 4. Which actions leave the DataFrame's own plan final.
if want("actions"):
    section("4. which actions run the DataFrame's own plan")
    for action in ("count", "show", "take", "first", "collect", "toPandas", "toArrow", "toLocalIterator"):
        df = trips.groupBy("payment_type").agg(F.count("*").alias("n"))
        import contextlib, io
        with contextlib.redirect_stdout(io.StringIO()):
            if action == "take":
                df.take(1)
            elif action == "toLocalIterator":
                list(df.toLocalIterator())
            else:
                getattr(df, action)()
        print(f"  {action:<16} isFinalPlan={'true' if 'isFinalPlan=true' in plan(df, 'simple') else 'false'}")

# 5. The AQE settings on this session.
if want("settings"):
    section("5. AQE settings, read from spark.conf on 4.3.0")
    for k in ("spark.sql.adaptive.enabled",
              "spark.sql.adaptive.coalescePartitions.enabled",
              "spark.sql.adaptive.coalescePartitions.parallelismFirst",
              "spark.sql.adaptive.coalescePartitions.minPartitionSize",
              "spark.sql.adaptive.coalescePartitions.maxReducerPartitionsPerTask",
              "spark.sql.adaptive.advisoryPartitionSizeInBytes",
              "spark.sql.adaptive.autoBroadcastJoinThreshold",
              "spark.sql.adaptive.localShuffleReader.enabled",
              "spark.sql.adaptive.skewJoin.enabled",
              "spark.sql.adaptive.skewJoin.skewedPartitionFactor",
              "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes",
              "spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold",
              "spark.sql.adaptive.convertSortMergeJoinToShuffledHashJoin.enabled",
              "spark.sql.adaptive.optimizer.excludedRules"):
        try:
            v = spark.conf.get(k)
        except Exception:
            v = "(unset)"
        print(f"  {k:<68} {v}")

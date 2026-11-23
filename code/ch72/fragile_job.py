"""Chapter 72, Step 11: a fragile job and a correct one, each run clean and then under chaos.

The job turns checkout events into charges: one row per "pay" event with a charge id, the
amount converted by a (fake) payment service, an audit sample, a ledger with one posting per
charge, and metrics. The chaos run uses local[4,4], tiny input splits, 7 shuffle partitions,
AQE off, a payment service that fails the first attempt of every task, and runs the job twice.

Run from code/:  uv run python ch72/fragile_job.py
"""
import json
import os
import shutil
import tempfile
from pathlib import Path

from pyspark import TaskContext
from pyspark.sql import Observation, functions as F
from pyspark.sql.types import DoubleType

from chaos import fresh

SRC = str(Path(__file__).resolve().parent / "data" / "checkout_events")
EVENTS = "event_id STRING, user STRING, action STRING, page STRING, ts TIMESTAMP, amount DOUBLE"
WORK = tempfile.mkdtemp(prefix="ch72-job-")
CHAOS_FLAG = f"{WORK}/chaos_on"           # a file every Python worker can see on this machine
SERVICE_LOG = f"{WORK}/service_calls.log"


def call_service(amount):
    """Convert CAD to USD. Under chaos, the first attempt of every task fails."""
    ctx = TaskContext.get()
    with open(SERVICE_LOG, "a") as f:
        f.write(f"{ctx.stageId()} {ctx.partitionId()} {ctx.attemptNumber()}\n")
    if os.path.exists(CHAOS_FLAG) and ctx.attemptNumber() == 0:
        raise ConnectionError("payment service timed out (injected)")
    return round(amount * 0.73, 2)


# ---- The fragile version -------------------------------------------------------------------
LEDGER = {"path": None, "calls": None}


def _fragile_to_usd(event_id, user, amount):
    usd = call_service(amount)
    LEDGER["calls"].add(1)                                   # counts every call, every action
    with open(LEDGER["path"], "a") as f:                     # posts from inside a task
        f.write(json.dumps({"charge_id": f"{event_id}-{user}", "amount_usd": usd}) + "\n")
    return usd


def fragile_job(spark, out, ledger):
    LEDGER["path"], LEDGER["calls"] = ledger, spark.sparkContext.accumulator(0)
    to_usd = F.udf(_fragile_to_usd, DoubleType())
    events = spark.read.schema(EVENTS).json(SRC)
    charges = (events.where("action = 'pay'").dropDuplicates(["event_id"])
               .withColumn("charge_id", F.expr("substr(uuid(), 1, 16)"))       # new every query
               .withColumn("amount_usd", to_usd("event_id", "user", "amount"))
               .select("charge_id", "event_id", "amount_usd"))
    revenue = charges.agg(F.round(F.sum("amount_usd"), 2)).first()[0]       # a second action
    charges.write.mode("append").parquet(f"{out}/charges")                  # a rerun duplicates
    spark.read.parquet(f"{out}/charges").sample(fraction=0.5, seed=42) \
        .write.mode("append").parquet(f"{out}/audit")                       # moves with partitions
    return {"charges": LEDGER["calls"].value, "revenue_usd": revenue}


# ---- The correct version -------------------------------------------------------------------
def post_ledger(spark, charges_dir, ledger):
    """Post each committed charge once, from the driver, keyed by charge_id, in one rename."""
    postings = {}
    if os.path.exists(ledger):
        postings = {p["charge_id"]: p["amount_usd"] for p in map(json.loads, open(ledger))}
    for r in spark.read.parquet(charges_dir).select("charge_id", "amount_usd").collect():
        postings[r.charge_id] = r.amount_usd
    with open(ledger + ".tmp", "w") as f:
        for cid in sorted(postings):
            f.write(json.dumps({"charge_id": cid, "amount_usd": postings[cid]}) + "\n")
    os.replace(ledger + ".tmp", ledger)


def correct_job(spark, out, ledger):
    to_usd = F.udf(call_service, DoubleType())                               # converts, nothing else
    events = spark.read.schema(EVENTS).json(SRC)
    charges = (events.where("action = 'pay'").dropDuplicates(["event_id"])
               .withColumn("charge_id", F.sha2(F.concat_ws("|", "event_id", "user"), 256).substr(1, 16))
               .withColumn("amount_usd", to_usd("amount"))
               .select("charge_id", "event_id", "amount_usd"))
    obs = Observation("charges")
    charges = charges.observe(obs, F.count(F.lit(1)).alias("charges"),
                              F.round(F.sum("amount_usd"), 2).alias("revenue_usd"))
    charges.write.mode("overwrite").parquet(f"{out}/charges")               # one action, replaces
    metrics = obs.get
    spark.read.parquet(f"{out}/charges").where(F.pmod(F.xxhash64("charge_id"), F.lit(2)) == 0) \
        .write.mode("overwrite").parquet(f"{out}/audit")                    # chosen by content
    post_ledger(spark, f"{out}/charges", ledger)
    return {"charges": metrics["charges"], "revenue_usd": metrics["revenue_usd"]}


# ---- Harness -------------------------------------------------------------------------------
def snapshot(spark, out, ledger, metrics):
    rows = lambda name: sorted(tuple(r) for r in spark.read.parquet(f"{out}/{name}").collect())
    posted = sorted((p["charge_id"], p["amount_usd"]) for p in map(json.loads, open(ledger)))
    return {"charges": rows("charges"), "audit": rows("audit"), "ledger": posted, "metrics": metrics}


def run(job):
    name = job.__name__
    print(f"{name}:")
    spark = fresh("local[2]")
    clean = snapshot(spark, f"{WORK}/{name}-clean", f"{WORK}/{name}-clean.jsonl",
                     job(spark, f"{WORK}/{name}-clean", f"{WORK}/{name}-clean.jsonl"))
    open(CHAOS_FLAG, "w").close()
    spark = fresh("local[4,4]", **{
        "spark.sql.files.maxPartitionBytes": "700", "spark.sql.files.openCostInBytes": "0",
        "spark.sql.shuffle.partitions": "7", "spark.sql.adaptive.enabled": "false"})
    out, ledger = f"{WORK}/{name}-chaos", f"{WORK}/{name}-chaos.jsonl"
    job(spark, out, ledger)
    chaos = snapshot(spark, out, ledger, job(spark, out, ledger))           # the rerun
    os.remove(CHAOS_FLAG)
    retried = {(s, p) for s, p, a in (map(int, l.split()) for l in open(SERVICE_LOG)) if a > 0}
    print(f"  chaos run: {len(retried)} tasks retried")
    for key in ("charges", "audit", "ledger"):
        a, b = clean[key], chaos[key]
        print(f"  {key:8} clean {len(a):2} rows, chaos {len(b):2} rows, identical: {a == b}")
    print(f"  metrics  clean {clean['metrics']}\n           chaos {chaos['metrics']}")
    os.remove(SERVICE_LOG)
    spark.stop()


run(fragile_job)
run(correct_job)
shutil.rmtree(WORK, ignore_errors=True)

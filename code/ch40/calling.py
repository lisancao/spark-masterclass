"""Chapter 40, part 2: call a remote classification service from Spark and count every request.

The service (ch40/service.py) runs on 127.0.0.1 in this driver process and fails on purpose:
429 over 16 requests a second, 413 over 20 items, "model busy" once for some items, slow answers.
Run from code/:  uv run python ch40/calling.py
"""
import shutil, sys, time
from collections import Counter
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from pyspark.sql import functions as F
from common.book import session, plan, ROOT, DATA
from ch40.service import (FakeClassifier, ServiceClient, RateLimiter, RetryableError, PermanentError,
                          backoff_seconds, idempotency_key)

TICKETS = ROOT / "code" / "data" / "messy" / "support_tickets"
WORK = DATA / "ch40" / "calling"
shutil.rmtree(WORK, ignore_errors=True)
spark = session("ch40-calling", spark_ui_showConsoleProgress="false", spark_sql_shuffle_partitions="4")
spark.sparkContext.setLogLevel("OFF")
RESULT = "key string, label string, model_version string, status string, attempts int, error string"

inbox = spark.read.schema("ticket_id string, channel string, text string").json(str(TICKETS / "inbox"))
keyed = inbox.withColumn("key", F.sha2(F.coalesce("text", F.lit("")), 256)).cache()
work = keyed.select("key", "text").dropDuplicates(["key"]).repartition(4).cache()
print(f"== {keyed.count()} tickets, {work.count()} distinct texts (content key = sha2 of the text)")


def report(svc, label):
    print(f"   {label}: {svc.requests} requests, status {dict(sorted(svc.status.items()))}, "
          f"largest request {svc.max_items} items, model runs {sum(svc.model_runs.values())} "
          f"for {len(svc.model_runs)} distinct texts")


# ---- 1. one request per row
svc = FakeClassifier(); url = svc.start()

@F.udf("string")
def classify_one(key, text):
    try:
        r = ServiceClient(url).classify([{"key": key, "text": text}], "kw-1", idempotency_key("kw-1", [key]))[0]
        return r.get("label") or "error: " + r["error"]
    except (RetryableError, PermanentError) as e:
        return "error: " + str(e)

print("\n== a UDF that calls the service once per row")
t0 = time.time()
res = work.select(classify_one("key", "text").alias("label")).collect()
print(f"   {time.time() - t0:.1f} s, rows with an error: {sum(r.label.startswith('error') for r in res)}"
      f" of {len(res)}")
report(svc, "service saw")
svc.stop()


# ---- 2. one client per partition, batches, a rate limit, retries with backoff, idempotency keys
def make_caller(url, batch=10, rate=2.0, max_attempts=4, version="kw-1"):
    def call(batches):
        client, limiter = ServiceClient(url), RateLimiter(rate)
        try:
            for pdf in batches:
                texts = dict(zip(pdf["key"], pdf["text"]))
                attempts, out, pending = Counter(), {}, list(texts)
                while pending:
                    chunk, pending = pending[:batch], pending[batch:]
                    for k in chunk:
                        attempts[k] += 1
                    limiter.wait()
                    try:
                        results = client.classify([{"key": k, "text": texts[k]} for k in chunk], version,
                                                  idempotency_key(version, chunk))
                    except RetryableError as e:
                        again = [k for k in chunk if attempts[k] < max_attempts]
                        out.update({k: (None, "failed", str(e)) for k in chunk if k not in again})
                        if again:
                            time.sleep(e.retry_after or backoff_seconds(max(attempts[k] for k in again)))
                            pending = again + pending
                        continue
                    except PermanentError as e:
                        out.update({k: (None, "failed", str(e)) for k in chunk})
                        continue
                    again = []
                    for r in results:
                        k = r["key"]
                        if "label" in r:
                            out[k] = (r["label"], "ok", None)
                        elif r.get("retryable") and attempts[k] < max_attempts:
                            again.append(k)
                        else:
                            out[k] = (None, "failed", r.get("error"))
                    if again:
                        time.sleep(backoff_seconds(max(attempts[k] for k in again)))
                        pending = again + pending
                keys = list(texts)
                yield pd.DataFrame({"key": keys, "label": [out[k][0] for k in keys], "model_version": version,
                                    "status": [out[k][1] for k in keys],
                                    "attempts": pd.array([attempts[k] for k in keys], dtype="int32"),
                                    "error": [out[k][2] for k in keys]})
        finally:
            client.close()
    return call


svc = FakeClassifier(); url = svc.start()
print("\n== mapInPandas: a client per partition, 10 items a request, 2 requests a second per task")
t0 = time.time()
results = work.mapInPandas(make_caller(url), RESULT).cache()
rows = results.collect()
print(f"   {time.time() - t0:.1f} s for {len(rows)} texts")
report(svc, "service saw")
print("   status and attempts:")
results.groupBy("status", "attempts", "error").count().orderBy("status", "attempts").show(truncate=False)
dup_runs = sum(1 for c in svc.model_runs.values() if c > 1)
print(f"   texts the model ran on more than once: {dup_runs} "
      f"(slow answers were retried with the same Idempotency-Key)")
svc.stop()

# ---- 3. rate per task times tasks
print("== the client limit is per task: total rate = per-task rate x concurrent tasks")
for parts, rate in ((4, 2.0), (16, 2.0), (16, 0.5)):
    svc = FakeClassifier(); url = svc.start()
    t0 = time.time()
    work.repartition(parts).mapInPandas(make_caller(url, rate=rate), RESULT).collect()
    print(f"   {parts:>2} partitions x {rate} req/s: {time.time() - t0:5.1f} s, {svc.requests:>3} requests, "
          f"429 responses {svc.status[429]:>3}")
    svc.stop()

# ---- 4. two writes from one mapInPandas result run the calls twice
print("\n== write once, then split")
svc = FakeClassifier(); url = svc.start()
res = work.mapInPandas(make_caller(url), RESULT)
res.where("status = 'ok'").write.parquet(str(WORK / "cache"))
res.where("status != 'ok'").write.parquet(str(WORK / "dead_letters"))
print(f"   two filtered writes of the same result: {svc.requests} requests")
svc.stop()
svc = FakeClassifier(); url = svc.start()
work.mapInPandas(make_caller(url), RESULT).write.parquet(str(WORK / "staging"))
staged = spark.read.parquet(str(WORK / "staging"))
staged.where("status = 'ok'").write.mode("overwrite").parquet(str(WORK / "cache"))
staged.where("status != 'ok'").write.mode("overwrite").parquet(str(WORK / "dead_letters"))
print(f"   one staging write, then two writes from the files: {svc.requests} requests")

# ---- 5. the next run sends only new work
print("\n== the next run: a left anti join against the cache and the dead letters")
done = (spark.read.parquet(str(WORK / "cache")).unionByName(spark.read.parquet(str(WORK / "dead_letters")))
        .where("model_version = 'kw-1'").select("key"))
new_tickets = spark.createDataFrame([(f"N{i:03d}", "email", t) for i, t in enumerate(
    ["my parcel never arrived", "please delete my account data", "the screen cracked after a week",
     "I want a refund for the blue jacket", "my parcel never arrived"])], "ticket_id string, channel string, text string")
everything = keyed.drop("key").unionByName(new_tickets).withColumn("key", F.sha2(F.coalesce("text", F.lit("")), 256))
todo = everything.select("key", "text").dropDuplicates(["key"]).join(done, "key", "left_anti")
print(f"   {everything.count()} tickets, {todo.count()} texts to send")
before = svc.requests
todo.repartition(1).mapInPandas(make_caller(url), RESULT).collect()
print(f"   requests for the second run: {svc.requests - before}")
print("   plan of the new-work query:", [op for op in ("BroadcastHashJoin", "SortMergeJoin", "LeftAnti")
                                         if op in plan(todo, "simple")])
svc.stop()

# ---- 6. validate structured answers from a model with VARIANT
print("\n== structured answers: validate before you trust them")
answers = spark.createDataFrame([
    ("a", '{"label": "shipping", "confidence": 0.92}'),
    ("b", '{"label": "refunds", "confidence": "high"}'),
    ("c", 'Sure! Here is the JSON: {"label": "privacy"}'),
    ("d", '{"label": "warranty"}')], "key string, answer string")
v = F.try_parse_json("answer")
checked = answers.select("key", F.try_variant_get(v, "$.label", "string").alias("label"),
                         F.try_variant_get(v, "$.confidence", "double").alias("confidence"),
                         F.when(v.isNull(), "not json")
                         .when(~F.try_variant_get(v, "$.label", "string").isin("shipping", "returns", "warranty",
                                                                               "privacy", "other"), "unknown label")
                         .when(F.try_variant_get(v, "$.confidence", "double").isNull(), "no confidence")
                         .alias("problem"))
checked.show(truncate=False)

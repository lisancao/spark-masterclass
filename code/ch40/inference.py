"""Chapter 40, part 1: run a model that lives in Python over a DataFrame.

A small naive Bayes model trained on the driver, then applied with a plain UDF, a pandas UDF,
an iterator pandas UDF and mapInPandas. Marker files count how many times each task loads the
model and how many rows the model really sees.
Run from code/:  uv run python ch40/inference.py
"""
import math, os, pickle, re, shutil, sys, time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Iterator
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from pyspark import TaskContext
from pyspark.sql import functions as F
from common.book import session, plan, ROOT, DATA

TICKETS = ROOT / "code" / "data" / "messy" / "support_tickets"
WORK = DATA / "ch40"
MARKS = WORK / "marks"
MODEL_FILE = WORK / "model.pkl"
shutil.rmtree(WORK, ignore_errors=True)
MARKS.mkdir(parents=True)
spark = session("ch40-inference", spark_ui_showConsoleProgress="false", spark_sql_shuffle_partitions="4")
spark.sparkContext.setLogLevel("OFF")


def tokenize(text):
    return re.findall(r"[a-z]+", (text or "").lower())


def train_naive_bayes(rows):
    """Multinomial naive Bayes with add-one smoothing, as plain dicts."""
    words, docs, vocab = defaultdict(Counter), Counter(), set()
    for text, label in rows:
        docs[label] += 1
        for w in tokenize(text):
            words[label][w] += 1
            vocab.add(w)
    total = sum(docs.values())
    return {"prior": {l: math.log(c / total) for l, c in docs.items()},
            "loglik": {l: {w: math.log((words[l][w] + 1) / (sum(words[l].values()) + len(vocab)))
                           for w in vocab} for l in docs},
            "unseen": {l: math.log(1 / (sum(words[l].values()) + len(vocab))) for l in docs}}


def predict_one(model, text):
    toks = tokenize(text)
    if not toks:
        return "other"
    return max(model["prior"], key=lambda l: model["prior"][l] + sum(
        model["loglik"][l].get(w, model["unseen"][l]) for w in toks))


def mark(kind, rows=0):
    """One file per call: kind, task attempt id, rows. Workers write them; the driver counts them."""
    ctx = TaskContext.get()
    tid = ctx.taskAttemptId() if ctx else -1
    with open(MARKS / f"{kind}-{tid}-{time.time_ns()}", "w") as f:
        f.write(str(rows))


def marks(kind):
    files = list(MARKS.glob(f"{kind}-*"))
    return len(files), sum(int(p.read_text()) for p in files)


def clear():
    for p in MARKS.iterdir():
        p.unlink()


train = spark.read.option("header", True).csv(str(TICKETS / "train.csv"))
rows = [(r.text, r.label) for r in train.collect()]
model = train_naive_bayes(rows)
MODEL_FILE.write_bytes(pickle.dumps(model))
print(f"== model trained on the driver from {len(rows)} labelled tickets, "
      f"{len(pickle.dumps(model)):,} bytes pickled, labels {sorted(model['prior'])}")

inbox = (spark.read.schema("ticket_id string, channel string, text string").json(str(TICKETS / "inbox"))
         .repartition(4, "ticket_id").cache())
truth = spark.read.option("header", True).csv(str(TICKETS / "inbox_labels.csv"))
print(f"   inbox: {inbox.count()} tickets in {inbox.rdd.getNumPartitions()} partitions")

# ---- 1. a plain UDF: the model travels in the closure, the function runs once per row
@F.udf("string")
def label_row(text):
    mark("row", 1)
    return predict_one(model, text)

clear()
out = inbox.select("ticket_id", label_row("text").alias("label"))
out.collect()
print("\n== plain @udf")
print("   plan:", [l.strip()[:60] for l in plan(out, "simple").splitlines() if "EvalPython" in l])
print("   calls of the function: %d (one per row)" % marks("row")[0])
out_noarrow = inbox.select(F.udf(lambda t: predict_one(model, t), "string", useArrow=False)("text"))
print("   useArrow=False plan:", [l.strip()[:60] for l in plan(out_noarrow, "simple").splitlines() if "EvalPython" in l])

# ---- 2. a scalar pandas UDF: one call per Arrow batch
@F.pandas_udf("string")
def label_batch(text: pd.Series) -> pd.Series:
    mark("batch", len(text))
    return text.map(lambda t: predict_one(model, t))

for size in (10000, 25):
    spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", size)
    clear()
    inbox.select(label_batch("text")).collect()
    calls, n = marks("batch")
    print(f"\n== pandas_udf, maxRecordsPerBatch={size}: {calls} calls for {n} rows") if size == 10000 else \
        print(f"== pandas_udf, maxRecordsPerBatch={size}: {calls} calls for {n} rows")
print("   plan:", [l.strip()[:60] for l in plan(inbox.select(label_batch("text")), "simple").splitlines()
                   if "EvalPython" in l])
print("   maxBytesPerBatch default:", spark.conf.get("spark.sql.execution.arrow.maxBytesPerBatch"))

# ---- 3. the iterator form: load once per task
@F.pandas_udf("string")
def label_iter(batches: Iterator[pd.Series]) -> Iterator[pd.Series]:
    m = pickle.loads(MODEL_FILE.read_bytes())   # once per task, not once per batch
    mark("load")
    for text in batches:
        yield text.map(lambda t: predict_one(m, t))

clear()
labelled = inbox.select("ticket_id", label_iter("text").alias("label")).collect()
print(f"\n== iterator pandas_udf, batches of 25: model loaded {marks('load')[0]} times for 4 partitions")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 10000)

# ---- 4. mapInPandas: any number of rows out, columns matched by name
def label_partition(batches):
    m = pickle.loads(MODEL_FILE.read_bytes())
    mark("load")
    for pdf in batches:
        yield pd.DataFrame({"ticket_id": pdf["ticket_id"], "label": pdf["text"].map(lambda t: predict_one(m, t)),
                            "confidence_bucket": "n/a"})

clear()
scored = inbox.mapInPandas(label_partition, "ticket_id string, label string, confidence_bucket string")
scored.collect()
print(f"== mapInPandas: model loaded {marks('load')[0]} times; plan:",
      [l.strip()[:40] for l in plan(scored, "simple").splitlines() if "MapInPandas" in l])
bad = inbox.mapInPandas(lambda it: (pd.DataFrame({"id": p["ticket_id"]}) for p in it), "ticket_id string")
try:
    bad.collect()
except Exception as e:
    print("   wrong column name:", re.search(r"\[([A-Z_.]+)\]", str(e)).group(1))

acc = (spark.createDataFrame(labelled).join(truth.withColumnRenamed("label", "expected"), "ticket_id")
       .agg(F.avg((F.col("label") == F.col("expected")).cast("double"))).first()[0])
print(f"   accuracy on the inbox against inbox_labels.csv: {acc:.3f}")

# ---- 5. how many times does the model run?
print("\n== how many times the model runs on 240 rows")
base = inbox.select("ticket_id", label_batch("text").alias("label"))
cases = {"collect()": lambda: base.collect(),
         "count()": lambda: base.count(),
         "limit(3).collect()": lambda: base.limit(3).collect(),
         "filter on the label": lambda: base.where("label != 'other'").collect(),
         "asNondeterministic + filter": lambda: inbox.select(
             "ticket_id", label_batch.asNondeterministic()("text").alias("label")).where("label != 'other'").collect()}
for name, run in cases.items():
    clear()
    run()
    print(f"   {name:<28} rows into the model: {marks('batch')[1]}")
print("   filter plan:", [l.strip()[:70] for l in plan(base.where("label != 'other'"), "simple").splitlines()
                         if "EvalPython" in l])
cached = base.cache()
clear(); cached.count(); first = marks("batch")[1]
clear(); cached.where("label != 'other'").collect(); second = marks("batch")[1]
print(f"   cache(), count(), then filter: {first} rows, then {second} rows")
cached.unpersist()

# ---- 6. predict_batch_udf keeps a model per worker process
from pyspark.ml.functions import predict_batch_udf
import numpy as np

def make_predict():
    m = pickle.loads(MODEL_FILE.read_bytes())
    mark("load")
    labels = sorted(m["prior"])
    def predict(texts: np.ndarray) -> np.ndarray:
        return np.array([labels.index(predict_one(m, t)) for t in texts], dtype=np.float32)
    return predict

clear()
pb = predict_batch_udf(make_predict, return_type="float", batch_size=50)
for _ in range(2):
    inbox.select(pb("text")).collect()
print(f"\n== predict_batch_udf over two queries: model loaded {marks('load')[0]} times")

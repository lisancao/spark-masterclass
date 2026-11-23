"""Chapter 54: words, TF-IDF and cosine search with MLlib, on the policies and the support tickets.

Run from code/:  uv run python ch54/text_search.py
"""
import math, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.ml import Pipeline
from pyspark.ml.feature import (CountVectorizer, HashingTF, IDF, Normalizer, RegexTokenizer,
                                StopWordsRemover, Tokenizer)
from pyspark.ml.functions import vector_to_array
from pyspark.sql import Window, functions as F
from common.book import session, plan, ROOT

MESSY = ROOT / "code" / "data" / "messy"
spark = session("ch54-search", spark_ui_showConsoleProgress="false")

docs = (spark.read.text(str(MESSY / "docs" / "*.md"), wholetext=True)
        .select(F.col("_metadata.file_name").alias("doc"), F.col("value").alias("text")))

# 0. wholetext=True gives one row per file. Without it, one row per line.
lines = spark.read.text(str(MESSY / "docs" / "*.md"))
print(f"== read: {lines.count()} rows as lines, {docs.count()} rows with wholetext=True")

# 1. Plain functions first.
print("\n== words without MLlib")
words = docs.select("doc", F.explode(F.split(F.lower("text"), r"[^a-z]+")).alias("word"))
words.filter(F.length("word") > 3).groupBy("word").count().orderBy(F.desc("count"), "word").show(4)
docs.select("doc", F.size(F.sentences("text")).alias("sentences")).orderBy("doc").show()

# 2. The two tokenizers on one sentence.
print("== tokenizers")
one = spark.createDataFrame([("Hello, order 1000 has a problem. Please help.",)], "text string")
print("Tokenizer:        ", Tokenizer(inputCol="text", outputCol="t").transform(one).first().t)
rt = RegexTokenizer(inputCol="text", outputCol="t", pattern=r"\W+")
print("RegexTokenizer:   ", rt.transform(one).first().t)
print("+ StopWordsRemover:", StopWordsRemover(inputCol="t", outputCol="w").transform(rt.transform(one)).first().w)
print("minTokenLength=4:  ", RegexTokenizer(inputCol="text", outputCol="t", pattern=r"\W+",
                                             minTokenLength=4).transform(one).first().t)
print("gaps=False, \\d+:  ", RegexTokenizer(inputCol="text", outputCol="t", pattern=r"\d+",
                                              gaps=False).transform(one).first().t)
print("default stop words:", len(StopWordsRemover.loadDefaultStopWords("english")), "English words")

# 3. TF-IDF on the four policies, and the IDF formula read back from the fitted model.
print("\n== TF-IDF")
stages = [RegexTokenizer(inputCol="text", outputCol="tokens", pattern=r"[^a-zA-Z]+"),
          StopWordsRemover(inputCol="tokens", outputCol="words")]
cv = CountVectorizer(inputCol="words", outputCol="tf")
idf_pipe = Pipeline(stages=stages + [cv, IDF(inputCol="tf", outputCol="tfidf")]).fit(docs)
vocab, idf = idf_pipe.stages[2].vocabulary, idf_pipe.stages[3].idf
df_counts = (Pipeline(stages=stages).fit(docs).transform(docs)
             .select(F.explode(F.array_distinct("words")).alias("w")).groupBy("w").count())
n_docs = docs.count()
for w in ("policy", "shipping", "batteries", "days"):
    d = df_counts.where(F.col("w") == w).first()["count"]
    print(f"  {w:<10} in {d} of {n_docs} docs   IDF {idf[vocab.index(w)]:.4f}"
          f"   log((N+1)/(df+1)) = {math.log((n_docs + 1) / (d + 1)):.4f}")

# 4. HashingTF has no vocabulary; collisions depend on numFeatures.
tickets = (spark.read.option("header", True).csv(str(MESSY / "support_tickets" / "train.csv"))
           .select(F.col("ticket_id").alias("id"), "text", "label")
           .unionByName(spark.read.schema("ticket_id string, channel string, text string")
                        .json(str(MESSY / "support_tickets" / "inbox"))
                        .join(spark.read.option("header", True)
                              .csv(str(MESSY / "support_tickets" / "inbox_labels.csv")), "ticket_id")
                        .select(F.col("ticket_id").alias("id"), "text", "label")))
vocab_all = (Pipeline(stages=stages).fit(tickets.na.fill("", ["text"])).transform(tickets.na.fill("", ["text"]))
             .select(F.explode("words").alias("w")).distinct())
n_words = vocab_all.count()
print(f"\n  {n_words} distinct words in {tickets.count()} tickets; slots used by HashingTF:")
for bits in (6, 8, 10, 12, 18):
    ht = HashingTF(inputCol="ws", outputCol="h", numFeatures=1 << bits)
    idx = [ht.indexOf(r.w) for r in vocab_all.collect()]
    print(f"    numFeatures 2^{bits:<2} = {1 << bits:>7}: {len(set(idx)):>3} slots, "
          f"{n_words - len(set(idx)):>3} words share a slot")

# 5. A search index over the policies.
print("\n== search the policies")
index = Pipeline(stages=stages + [HashingTF(inputCol="words", outputCol="tf", numFeatures=1 << 12),
                                  IDF(inputCol="tf", outputCol="tfidf"),
                                  Normalizer(inputCol="tfidf", outputCol="vec", p=2.0)]).fit(docs)
indexed = index.transform(docs)
dot_udf = F.udf(lambda a, b: float(a.dot(b)), "double")
queries = ["how long does express shipping take", "can I return an unopened item for a refund",
           "battery warranty", "returned items refunded", "pizza"]
for q in queries:
    qv = index.transform(spark.createDataFrame([(q,)], "text string")).select(F.col("vec").alias("q"))
    ranked = indexed.crossJoin(qv).select("doc", dot_udf("vec", "q").alias("score")).orderBy(F.desc("score"), "doc")
    top = ranked.collect()
    print(f"  {q:<44} best {top[0].doc:<12} {top[0].score:.4f}   others {[round(r.score, 4) for r in top[1:]]}")

# 6. Many queries at once: every inbox ticket against the labelled training tickets.
print("\n== every inbox ticket against 160 labelled tickets")
train = tickets.where(F.col("id").startswith("T"))
inbox = tickets.where(F.col("id").startswith("I")).na.fill("", ["text"])
tix = Pipeline(stages=stages + [HashingTF(inputCol="words", outputCol="tf", numFeatures=1 << 12),
                                IDF(inputCol="tf", outputCol="tfidf"),
                                Normalizer(inputCol="tfidf", outputCol="vec")]).fit(train)
a = tix.transform(inbox).select(F.col("id").alias("q"), F.col("label").alias("expected"), F.col("vec").alias("qv"))
b = tix.transform(train).select("id", "label", "vec")
best = Window.partitionBy("q").orderBy(F.desc("score"), "id")

def nearest(score_col):
    pairs = a.crossJoin(b).select("q", "expected", "id", "label", score_col.alias("score"))
    return pairs.withColumn("rank", F.row_number().over(best)).where("rank = 1")

# The same score two ways: a Python UDF over MLlib vectors, or the 4.3 vector_inner_product
# over float arrays, with no Python worker.
as_float = lambda c: vector_to_array(c, "float32")
ways = {"Python UDF a.dot(b)": dot_udf("qv", "vec"),
        "F.vector_inner_product": F.vector_inner_product(as_float("qv"), as_float("vec"))}
for name, col in ways.items():
    q = nearest(col).cache()
    times = []
    for _ in range(3):
        q.unpersist(); q = nearest(col)
        t0 = time.time(); rows = q.collect(); times.append(time.time() - t0)
    right = sum(r.label == r.expected for r in rows)
    print(f"  {name:<24} {len(rows)} queries x {b.count()} = {len(rows) * b.count():,} scores, "
          f"median {sorted(times)[1]:.2f} s, nearest label right for {right}/{len(rows)}")
    txt = plan(q, "simple")
    found = [op for op in ("ArrowEvalPython", "BatchEvalPython", "BroadcastNestedLoopJoin",
                           "vector_inner_product") if op in txt]
    print("     plan has:", found)

no_words = inbox.where(F.size(F.filter(F.split(F.lower("text"), "[^a-z]+"), lambda w: w != "")) == 0).count()
print(f"  inbox tickets with no words at all: {no_words} (their vector is all zeros, every score is 0)")

"""Chapter 38: near-duplicate messages with MinHashLSH, and the traps around it.

Run from code/:  uv run python ch38/near_duplicates.py
Over Connect:    SPARK_REMOTE=sc://localhost:10270 uv run python ch38/near_duplicates.py
"""
import os, sys, textwrap
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.ml.feature import HashingTF, MinHashLSH, NGram, RegexTokenizer
from pyspark.sql import functions as F
from common.book import session, plan, ROOT

MESSY = ROOT / "code" / "data" / "messy"
spark = session("ch38-dups", spark_ui_showConsoleProgress="false")
print("session:", type(spark).__module__)

CHATS = [("chat_0", "Where is my order 1042? It has not arrived yet."),
         ("chat_1", "Where is my order 1042?? It has not arrived yet!"),
         ("chat_2", "Do you ship express to Vietnam?")]
mail = spark.read.text(str(MESSY / "mail" / "*.eml"), wholetext=True).select(
    F.regexp_extract("_metadata.file_name", r"^(.*)\.eml$", 1).alias("id"),
    F.trim(F.regexp_extract("value", r"\r?\n\r?\n(.*)", 1)).alias("text"))
messages = mail.unionByName(spark.createDataFrame(CHATS, "id string, text string"))
messages.orderBy("id").show(truncate=False)

tok = RegexTokenizer(inputCol="text", outputCol="tokens", pattern=r"\W+")
tf = HashingTF(inputCol="tokens", outputCol="features", numFeatures=1 << 10, binary=True)
features = tf.transform(tok.transform(messages))
model = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=5, seed=7).fit(features)
print("hashes of ticket_0:", [int(v[0]) for v in model.transform(features).where("id = 'ticket_0'").first().hashes])

for threshold in (0.1, 0.3):
    pairs = (model.approxSimilarityJoin(features, features, threshold)
             .filter("datasetA.id < datasetB.id")
             .select(F.col("datasetA.id").alias("a"), F.col("datasetB.id").alias("b"),
                     F.round("distCol", 4).alias("distance")).orderBy("a", "b"))
    rows = pairs.collect()
    print(f"threshold {threshold}: {len(rows)} pairs", [(r.a, r.b, r.distance) for r in rows])

# One query against all messages: approxNearestNeighbors.
key = features.where("id = 'chat_0'").first().features
nn = model.approxNearestNeighbors(features, key, 4).select("id", F.round("distCol", 4).alias("d")).collect()
print("4 nearest to chat_0:", [(r.id, r.d) for r in nn])

# The exact Jaccard distance of one pair, by hand.
t = {r.id: set(r.tokens) for r in tok.transform(messages).collect()}
x, y = t["ticket_0"], t["ticket_1"]
print(f"ticket_0 vs ticket_1 by hand: 1 - {len(x & y)}/{len(x | y)} = {1 - len(x & y) / len(x | y):.4f}")

# Trap 1: a message with no tokens.
inbox = spark.read.schema("ticket_id string, channel string, text string").json(str(MESSY / "support_tickets" / "inbox"))
itok = tok.transform(inbox.na.fill("", ["text"]))
print("\ninbox tickets with no tokens:", itok.where(F.size("tokens") == 0).count(), "of", inbox.count())
try:
    f2 = tf.transform(itok)
    MinHashLSH(inputCol="features", outputCol="hashes", seed=7).fit(f2).transform(f2).collect()
except Exception as e:
    m = str(e)
    print("MinHash over them ->", textwrap.shorten(m.split("\n")[0], 88))
    i = m.find("Must have")
    print("   cause:", m[i:i + 40] if i >= 0 else "(not in message)")

clean = tf.transform(itok.where(F.size("tokens") > 0))
im = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=5, seed=7).fit(clean)
dups = (im.approxSimilarityJoin(clean, clean, 0.01).filter("datasetA.ticket_id < datasetB.ticket_id")
        .select(F.col("datasetA.text").alias("text")).groupBy("text").count().orderBy(F.desc("count")))
print("inbox pairs at distance < 0.01 (same word set), by text:")
dups.show(5, truncate=60)

# Trap 2: the distCol argument.
cols = model.approxSimilarityJoin(features, features, 0.3, distCol="jaccard").columns
print("approxSimilarityJoin(..., distCol='jaccard').columns ->", cols)

# Word sets ignore order; shingles (word 2-grams) do not.
pair = spark.createDataFrame([("p", "refund the order please not the shipping"),
                              ("q", "the shipping please not the order refund")], "id string, text string")
w = tok.transform(pair)
for name, col in (("word set", "tokens"), ("word 2-grams", "grams")):
    v = HashingTF(inputCol=col, outputCol="features", binary=True).transform(
        NGram(n=2, inputCol="tokens", outputCol="grams").transform(w))
    d = (MinHashLSH(inputCol="features", outputCol="h", numHashTables=5, seed=7).fit(v)
         .approxSimilarityJoin(v, v, 1.01).where("datasetA.id = 'p' AND datasetB.id = 'q'")
         .select("distCol").first())
    print(f"  {name:<13} distance p-q: {d.distCol:.4f}" if d else f"  {name:<13} no candidate pair")

# What the similarity join runs.
print()
print("\n".join(l[:150] for l in plan(model.approxSimilarityJoin(features, features, 0.3), "simple").splitlines()))

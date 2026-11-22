"""Chapter 40: entity resolution over three customer systems, measured against an answer key.

Standardize, block, score, cluster (label propagation with localCheckpoint), survive.
Classic only for the plan measurements (they read the plan through df._jdf).
Run from code/:  uv run python ch40/resolve.py
"""
import csv, sys, time, unicodedata
from collections import defaultdict
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from pyspark.sql import Window, functions as F
from common.book import session, ROOT

ENT = ROOT / "code" / "data" / "messy" / "entities"
spark = session("ch40-resolve", spark_ui_showConsoleProgress="false", spark_sql_shuffle_partitions="4")

NICKNAMES = {"bob": "robert", "rob": "robert", "bill": "william", "will": "william", "liz": "elizabeth",
             "beth": "elizabeth", "kate": "katherine", "kathy": "katherine", "mike": "michael",
             "jen": "jennifer", "tom": "thomas", "maggie": "margaret", "peggy": "margaret", "dan": "daniel",
             "chris": "christopher", "alex": "alexander", "nick": "nicholas", "jon": "jonathan",
             "sam": "samantha", "ben": "benjamin"}


def strip_accents(s):
    if s is None:
        return None
    s = s.replace("Đ", "D").replace("đ", "d")          # Đ is its own letter, not D plus a mark
    return "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))


def jaro_winkler(s1, s2, p=0.1):
    if s1 == s2:
        return 1.0
    l1, l2 = len(s1), len(s2)
    if not l1 or not l2:
        return 0.0
    win = max(max(l1, l2) // 2 - 1, 0)
    u1, u2, m = [False] * l1, [False] * l2, 0
    for i, ch in enumerate(s1):
        for j in range(max(0, i - win), min(l2, i + win + 1)):
            if not u2[j] and s2[j] == ch:
                u1[i] = u2[j] = True
                m += 1
                break
    if not m:
        return 0.0
    k = t = 0
    for i in range(l1):
        if u1[i]:
            while not u2[k]:
                k += 1
            t += s1[i] != s2[k]
            k += 1
    jaro = (m / l1 + m / l2 + (m - t / 2) / m) / 3
    pre = 0
    for x, y in zip(s1[:4], s2[:4]):
        if x != y:
            break
        pre += 1
    return jaro + pre * p * (1 - jaro)


def name_similarity(a, b):
    if a is None or b is None or not len(a) or not len(b):
        return None
    one = lambda x, y: sum(max(jaro_winkler(s, t) for t in y) for s in x) / len(x)
    return min(one(list(a), list(b)), one(list(b), list(a)))


@F.pandas_udf("string")
def unaccent(s: pd.Series) -> pd.Series:
    return s.map(strip_accents)


@F.pandas_udf("double")
def name_sim(a: pd.Series, b: pd.Series) -> pd.Series:
    return pd.Series([name_similarity(x, y) for x, y in zip(a, b)], dtype="float64")


# ---- answer key
with open(ENT / "truth.csv") as f:
    truth = {f"{r['source']}:{r['record_id']}": r["entity_id"] for r in csv.DictReader(f)}
members = defaultdict(list)
for rid, e in truth.items():
    members[e].append(rid)
true_pairs = {tuple(sorted((a, b))) for ids in members.values() for a in ids for b in ids if a < b}
tp_df = spark.createDataFrame(sorted(true_pairs), "l string, r string").cache()

# ---- Step 1: one schema
crm = (spark.read.option("header", True).csv(str(ENT / "crm.csv"))
       .select(F.lit("crm").alias("source"), F.col("crm_id").alias("record_id"),
               F.concat_ws(" ", "first_name", "last_name").alias("name"), "email", "phone", "city",
               F.to_date("updated_at").alias("updated_at")))
parts = F.split("name", ",")
shop = (spark.read.option("header", True).csv(str(ENT / "shop.csv"))
        .select(F.lit("shop").alias("source"), F.col("customer_no").alias("record_id"),
                F.when(F.col("name").contains(","), F.concat_ws(" ", F.trim(parts[1]), F.trim(parts[0])))
                .otherwise(F.col("name")).alias("name"),
                "email", "phone", "city", F.to_date("last_order").alias("updated_at")))
support = (spark.read.schema("contact_id string, name string, email string, phone string, seen timestamp")
           .json(str(ENT / "support.jsonl"))
           .select(F.lit("support").alias("source"), F.col("contact_id").alias("record_id"), "name", "email",
                   "phone", F.lit(None).cast("string").alias("city"), F.to_date("seen").alias("updated_at")))
records = crm.unionByName(shop).unionByName(support).cache()
n = records.count()
print(f"== {n} records: " + ", ".join(f"{r.source} {r['count']}" for r in
                                       records.groupBy("source").count().orderBy("source").collect()))
print(f"   answer key: {len(members)} people, {len(true_pairs)} true pairs, {n * (n - 1) // 2:,} pairs in all")
print(f"   dropDuplicates(): {records.dropDuplicates().count()} rows, "
      f"on (name, email, phone): {records.dropDuplicates(['name', 'email', 'phone']).count()} rows")

# ---- Step 2: matching keys
nick = F.create_map(*[F.lit(x) for kv in NICKNAMES.items() for x in kv])
raw_tokens = F.split(F.regexp_replace(F.lower(unaccent("name")), "[^a-z ]", ""), " +")
tokens = F.transform(F.filter(raw_tokens, lambda t: t != ""), lambda t: F.coalesce(F.element_at(nick, t), t))
email = F.nullif(F.lower(F.trim("email")), F.lit(""))
local, domain = F.split(email, "@")[0], F.split(email, "@")[1]
norm = (records.withColumn("id", F.concat_ws(":", "source", "record_id"))
        .withColumn("tokens", F.array_sort(tokens))
        .withColumn("email_key", F.when(domain == "gmail.com", F.concat(
            F.regexp_replace(F.split(local, "\\+")[0], "\\.", ""), F.lit("@gmail.com"))).otherwise(email))
        .withColumn("phone_key", F.nullif(F.right(F.regexp_replace("phone", "[^0-9]", ""), F.lit(10)), F.lit("")))
        .withColumn("city_key", F.lower(unaccent("city")))).cache()
print("\n== matching keys")
norm.where("id in ('crm:CRM-0001', 'shop:10001', 'support:S-001')") \
    .select("id", "name", "tokens", "email_key", "phone_key").orderBy("id").show(truncate=False)
print("   element_at(map, missing key) under ANSI:",
      spark.range(1).select(F.element_at(nick, F.lit("zuma"))).first()[0],
      "| soundex('Đặng') =", spark.range(1).select(F.soundex(F.lit("Đặng"))).first()[0],
      "| soundex('Dang') =", spark.range(1).select(F.soundex(F.lit("Dang"))).first()[0])
print("   levenshtein('garcia', 'garccia') =",
      spark.range(1).select(F.levenshtein(F.lit("garcia"), F.lit("garccia"))).first()[0],
      "| with threshold 0:", spark.range(1).select(F.levenshtein(F.lit("garcia"), F.lit("garccia"), 0)).first()[0])
print(f"   jaro_winkler('martha', 'marhta') = {jaro_winkler('martha', 'marhta'):.4f}, "
      f"name_similarity(['an','nguyen','van'], ['an','nguyen']) = "
      f"{name_similarity(['an', 'nguyen', 'van'], ['an', 'nguyen']):.4f}")


# ---- Step 3: blocking
def pairs_on(keyed):
    k = keyed.where(F.col("bk").isNotNull())
    return (k.alias("a").join(k.alias("b"), "bk").where(F.col("a.id") < F.col("b.id"))
            .select(F.col("a.id").alias("l"), F.col("b.id").alias("r")))


passes = {"email": pairs_on(norm.select("id", F.col("email_key").alias("bk"))),
          "phone": pairs_on(norm.select("id", F.col("phone_key").alias("bk"))),
          "soundex of a token": pairs_on(norm.select("id", F.explode(F.filter("tokens", lambda t: F.length(t) > 1))
                                                     .alias("t")).select("id", F.soundex("t").alias("bk")))}
print("\n== blocking: candidates and blocking recall")
acc = None
for name, p in passes.items():
    acc = p if acc is None else acc.unionByName(p)
    got = {(r.l, r.r) for r in p.distinct().collect()}
    print(f"   {name:<20} {len(got):>4} pairs, finds {len(got & true_pairs):>3} of {len(true_pairs)} true pairs")
cands = acc.distinct().cache()
got = {(r.l, r.r) for r in cands.collect()}
print(f"   all three passes     {len(got):>4} pairs ({len(got) / (n * (n - 1) // 2):.1%} of all), "
      f"finds {len(got & true_pairs)} of {len(true_pairs)}")
sizes = (norm.select("id", F.explode(F.filter("tokens", lambda t: F.length(t) > 1)).alias("t"))
         .select("t", F.soundex("t").alias("code")).groupBy("code")
         .agg(F.count("*").alias("records"), F.array_sort(F.collect_set("t")).alias("tokens"))
         .orderBy(F.desc("records")))
print("   largest soundex blocks:")
for r in sizes.limit(3).collect():
    print(f"     {r.code}  {r.records:>3} records  {r.tokens[:6]}")

# ---- Step 4: scoring
cols = ["id", "tokens", "email_key", "phone_key", "city_key"]
pairs = (cands.join(norm.select([F.col(c).alias(f"l_{c}") for c in cols]), F.col("l") == F.col("l_id"))
         .join(norm.select([F.col(c).alias(f"r_{c}") for c in cols]), F.col("r") == F.col("r_id"))
         .withColumn("name_sim", name_sim("l_tokens", "r_tokens"))).cache()


def same(key):
    return F.col(f"l_{key}") == F.col(f"r_{key}")


def evidence(key, agree, disagree):
    l, r = F.col(f"l_{key}"), F.col(f"r_{key}")
    return F.when(l.isNull() | r.isNull(), 0.0).when(l == r, agree).otherwise(-disagree)


naive = (0.5 * F.col("name_sim") + 0.3 * F.coalesce(same("email_key").cast("double"), F.lit(0.0))
         + 0.2 * F.coalesce(same("phone_key").cast("double"), F.lit(0.0)))
scored = pairs.select("l", "r", "name_sim", naive.alias("naive"),
                      (F.col("name_sim") + evidence("email_key", 0.5, 0.3) + evidence("phone_key", 0.3, 0.2)
                       + evidence("city_key", 0.1, 0.05)).alias("score")).cache()


def pr(col, t):
    pred = {(r.l, r.r) for r in scored.where(F.col(col) >= t).collect()}
    tp = len(pred & true_pairs)
    return (tp / len(pred) if pred else 0.0), tp / len(true_pairs), len(pred)


print("\n== scoring: a missing value is not a disagreement")
p1, r1, _ = pr("naive", 0.6)
p2, r2, _ = pr("score", 0.9)
print(f"   weighted sum, missing = 0 (threshold 0.6): precision {p1:.3f} recall {r1:.3f}")
print(f"   evidence, missing neutral (threshold 0.9): precision {p2:.3f} recall {r2:.3f}")
print("   threshold sweep on the evidence score:")
for t in (0.7, 0.8, 0.9, 1.0, 1.1, 1.3):
    p, r, k = pr("score", t)
    print(f"     >= {t:<4} {k:>4} matches  precision {p:.3f}  recall {r:.3f}")
wrong = scored.where("score >= 0.9").join(tp_df.withColumn("t", F.lit(1)), ["l", "r"], "left").where("t IS NULL")
for r in wrong.collect():
    names = {x.id: x.name for x in norm.where(F.col("id").isin(r.l, r.r)).collect()}
    print(f"   false match: {names[r.l]!r} ({r.l}) and {names[r.r]!r} ({r.r}), score {r.score:.3f}")

# ---- Step 5: clustering with label propagation, and the plan it builds
edges = scored.where("score >= 0.9").select("l", "r").cache()
both = edges.select(F.col("l").alias("src"), F.col("r").alias("dst")).unionByName(
    edges.select(F.col("r").alias("src"), F.col("l").alias("dst"))).cache()
ids = norm.select("id")


def propagate(cut, rounds_max=12, show=True):
    labels = ids.select("id", F.col("id").alias("label"))
    if cut:
        labels = labels.localCheckpoint()
    for rnd in range(1, rounds_max + 1):
        offer = (both.join(labels.withColumnRenamed("id", "src"), "src")
                 .groupBy(F.col("dst").alias("id")).agg(F.min("label").alias("offer")))
        new = labels.join(offer, "id", "left").select(
            "id", F.least("label", F.coalesce("offer", "label")).alias("label"))
        if cut:
            new = new.localCheckpoint()
        qe = new._jdf.queryExecution()
        t0 = time.time()
        changed = new.join(labels.withColumnRenamed("label", "old"), "id").where("label != old").count()
        secs = time.time() - t0
        if show:
            print(f"     round {rnd}: changed {changed:>3}, analyzed plan {qe.analyzed().treeString().count(chr(10)):>5}"
                  f" lines, {qe.analyzed().collectLeaves().length():>4} leaves, count() {secs:.2f} s")
        labels = new
        if changed == 0:
            return labels, rnd
    return labels, rounds_max


print("\n== label propagation without a cut")
_, rounds = propagate(cut=False)
print("\n== label propagation with localCheckpoint() each round")
labels, rounds = propagate(cut=True)
ents = labels.select("label").distinct().count()
print(f"   {ents} entities after {rounds} rounds (the answer key has {len(members)})")

clustered = {}
for r in labels.collect():
    clustered.setdefault(r.label, []).append(r.id)
cl_pairs = {tuple(sorted((a, b))) for v in clustered.values() for a in v for b in v if a < b}
tp = len(cl_pairs & true_pairs)
print(f"   pairs implied by the clusters: {len(cl_pairs)}, precision {tp / len(cl_pairs):.3f}, "
      f"recall {tp / len(true_pairs):.3f}")
print(f"   pairs the matcher called directly: {p2:.3f} precision")
big = max(clustered.values(), key=len)
print(f"   largest cluster: {len(big)} records, people in it per the key: {sorted({truth[i] for i in big})}")

try:
    spark.conf.set("spark.checkpoint.dir", "/tmp/x")
    print("   spark.conf.set('spark.checkpoint.dir') at runtime: accepted")
except Exception as e:
    print("   spark.conf.set('spark.checkpoint.dir') at runtime:", str(e).split("]")[0] + "]")

# ---- Step 6: survivorship
latest = lambda c: F.max_by(c, F.when(c.isNotNull(), F.col("updated_at")))
rows = norm.join(labels, "id").withColumn("clean", F.initcap(F.trim(F.regexp_replace("name", r"\s+", " "))))
gold = rows.groupBy("label").agg(
    F.mode(F.array_join("tokens", " "), deterministic=True).alias("name_key"),
    latest(F.col("email_key")).alias("email"),
    F.mode("phone_key", deterministic=True).alias("phone"),
    latest(F.col("city")).alias("city"),
    F.array_sort(F.collect_set("source")).alias("sources"),
    F.count("*").alias("records"))
print("\n== golden records (largest entities)")
gold.orderBy(F.desc("records"), "label").show(4, truncate=False)
print("   max_by(x, NULL ordering) ignores the row:",
      spark.createDataFrame([("a", 1), ("b", None)], "x string, o int").agg(F.max_by("x", "o")).first()[0])

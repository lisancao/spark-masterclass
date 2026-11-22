"""Chapter 6: strings. Whitespace, case, regular expressions, accents, collations and UTF-8.

Run from code/:  uv run python ch06/strings.py
"""
import re
import shutil
import sys
import tempfile
import time
import unicodedata
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from common.book import session, plan

DATA = Path(__file__).resolve().parents[1] / "data"
tmp = Path(tempfile.mkdtemp(prefix="ch06-"))
spark = session("ch06-strings", spark_sql_session_timeZone="UTC", spark_sql_warehouse_dir=str(tmp))


def error_line(e):
    for line in str(e).splitlines():
        m = re.search(r"\[[A-Z_.]+\].*", line)
        if m:
            return m.group(0)
    return str(e).splitlines()[0]


def attempt(f, width=96):
    try:
        return f()
    except Exception as e:
        return "ERROR " + error_line(e)[:width]


# 1. Whitespace: trim removes U+0020 and nothing else.
print("== 1. whitespace")
names = spark.createDataFrame([("  ana   maria  ",), ("\tBinh\n",), (" Chloé ",)], "s STRING")
names.select(F.to_json(F.struct("s")).alias("s"), F.length("s").alias("len"),
             F.length(F.trim("s")).alias("trim"),
             F.length(F.trim(F.regexp_replace("s", r"\s+", " "))).alias("re_s"),
             F.length(F.trim(F.regexp_replace("s", r"[\s ]+", " "))).alias("re_nbsp"),
             F.length(F.trim(F.regexp_replace("s", r"(?U)\s+", " "))).alias("re_unicode")).show()
print("trim(s, ' \\t\\n') with a string:", attempt(lambda: names.select(F.trim("s", " \t\n")).collect(), 70))
print("trim(s, lit(...)):", [r[0] for r in names.select(F.trim("s", F.lit(" \t\n "))).collect()])
print(spark.sql("SELECT TRIM(LEADING '0' FROM '000120') AS a, btrim('xxhixx', 'x') AS b").first())

# 2. Case.
print("\n== 2. case")
(spark.createDataFrame([("BINH TRAN",), ("o'neil",), ("mary-jane van dyke",), ("McDONALD",)], "s STRING")
 .select("s", F.initcap("s"), F.lower("s")).show())

# 3. regexp_replace and translate.
print("== 3. regexp_replace and translate")
phones = spark.createDataFrame([("(416) 555-0199",), ("416.555.0123",), ("+1 416 555 0147",), ("n/a",)], "p STRING")
phones.select("p", F.regexp_replace("p", r"[^0-9]", "").alias("digits"),
              F.translate("p", "()-. +", "").alias("translated")).show()
print(spark.createDataFrame([("Nguyen, Linh",)], "n STRING")
      .select(F.regexp_replace("n", r"^(\w+), (\w+)$", "$2 $1").alias("swapped")).first())


# 4. Accents: translate, or a Python UDF with unicodedata. No built-in unaccent in 4.3.
@F.udf(StringType())
def strip_accents(s):
    if s is None:
        return None
    return "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))


print("\n== 4. accents")
people = spark.createDataFrame([(s,) for s in ["Montréal", "Nguyễn Thị Hà", "Zoë", "Đặng", "ﬁancé", None]], "s STRING")
people.select("s", strip_accents("s").alias("udf_nfkd"),
              F.translate("s", "éễịàëặĐ", "eeiaeaD").alias("translate")).show()
fn = set(r.function for r in spark.sql("SHOW FUNCTIONS").collect())
print("built-in unaccent/normalize:", sorted(f for f in fn if "accent" in f or "normaliz" in f) or "none")
print([l.strip() for l in plan(people.select(strip_accents("s")), "simple").splitlines() if "Eval" in l][0])

# 5. Collations.
print("\n== 5. collations")
spark.sql("""SELECT 'Montréal' = 'MONTREAL' AS plain,
  'Montréal' COLLATE UTF8_LCASE = 'MONTRÉAL' AS lcase,
  'Montréal' COLLATE UNICODE_CI = 'montréal' AS ci,
  'Montréal' COLLATE UNICODE_CI_AI = 'montreal' AS ci_ai""").show()

deliveries = spark.read.option("header", True).csv(str(DATA / "geo_deliveries" / "deliveries.csv"))
print("deliveries:", deliveries.count(), "rows,", deliveries.select("city").distinct().count(), "raw spellings")
print(f"  {'collation':<20} {'distinct':>8} {'trim first':>11}")
for coll in ["UTF8_BINARY", "UTF8_LCASE", "UNICODE_CI", "UNICODE_CI_AI", "UNICODE_CI_AI_RTRIM"]:
    a = deliveries.select(F.collate("city", coll)).distinct().count()
    b = deliveries.select(F.collate(F.trim("city"), coll)).distinct().count()
    print(f"  {coll:<20} {a:>8} {b:>11}")

print("\nthe same letters, different bytes:")
(deliveries.filter(F.collate(F.trim("city"), "UNICODE_CI_AI") == "montreal")
 .groupBy(F.collate("city", "UTF8_LCASE").alias("utf8_lcase"))
 .agg(F.count("*").alias("n"), F.hex(F.encode(F.first("city"), "UTF-8")).alias("bytes"))
 .orderBy("bytes").show(truncate=False))

spark.sql("""SELECT 'Vancouver  ' = 'vancouver' COLLATE UNICODE_CI_RTRIM AS trailing,
                    ' Vancouver'  = 'vancouver' COLLATE UNICODE_CI_RTRIM AS leading""").show()
spark.sql("""SELECT 'Straße' = 'STRASSE' COLLATE UNICODE_CI AS ci, 'Straße' = 'STRASSE' COLLATE UNICODE_CI_AI AS ci_ai,
  'Đà Nẵng' = 'da nang' COLLATE UNICODE_CI_AI AS root_ai, 'Đà Nẵng' = 'da nang' COLLATE vi_CI_AI AS vi_ai,
  'Hà Nội' = 'HÀ NỘI' COLLATE vi_CI_AI AS vi_ci""").show()
words = spark.createDataFrame([(w,) for w in ["b", "A", "é", "a", "B", "f", "e"]], "w STRING")
print("ORDER BY bytes:  ", [r.w for r in words.orderBy("w").collect()])
print("ORDER BY UNICODE:", [r.w for r in words.orderBy(F.collate("w", "UNICODE")).collect()])
print("collations():", spark.sql("SELECT count(*) FROM collations()").first()[0],
      "| ICU", spark.sql("SELECT DISTINCT ICU_VERSION FROM collations() WHERE ICU_VERSION IS NOT NULL").first()[0])
spark.sql("SHOW COLLATIONS LIKE 'UNICODE_CI*'").show(truncate=False)

print("where collations stop:")
c = F.collate("city", "UNICODE_CI_AI")
limits = [
    ("collect_set", lambda: deliveries.groupBy(c.alias("k")).agg(F.collect_set(c)).collect()),
    ("LIKE, UNICODE_CI_AI", lambda: spark.sql("SELECT 'Montréal' COLLATE UNICODE_CI_AI LIKE 'mont%'").first()),
    ("LIKE, UTF8_LCASE", lambda: spark.sql("SELECT 'Montréal' COLLATE UTF8_LCASE LIKE 'mont%'").first()),
    ("two explicit collations", lambda: spark.sql("SELECT 'a' COLLATE UTF8_LCASE = 'A' COLLATE UNICODE_CI").first()),
    ("misspelt name", lambda: spark.sql("SELECT 'a' COLLATE UNICODE_CIAI").first()),
    ("hash equal?", lambda: spark.sql("SELECT hash('Montréal' COLLATE UNICODE_CI_AI) = "
                                      "hash('MONTREAL' COLLATE UNICODE_CI_AI)").first()),
]
for label, f in limits:
    print(f"  {label:<24} {str(attempt(f, 84))[:96]}")

# Grouping: one label per group, chosen by Spark.
deliveries.groupBy(F.collate(F.trim("city"), "UNICODE_CI_AI").alias("city")).count().orderBy("city").show()

# A table with a default collation, and a collated join.
spark.sql("CREATE TABLE delivery_cities (city STRING, n INT) USING parquet DEFAULT COLLATION UNICODE_CI_AI")
spark.sql("INSERT INTO delivery_cities VALUES ('Montréal', 1), ('MONTREAL', 2), ('Hà Nội', 3), ('ha noi', 4)")
spark.sql("SELECT city, sum(n) AS n FROM delivery_cities GROUP BY city ORDER BY n").show()
print("WHERE city = 'montreal':", [tuple(r) for r in spark.sql("SELECT * FROM delivery_cities WHERE city = 'montreal'").collect()])
print("schema read back:", spark.table("delivery_cities").schema["city"].dataType)
stores = spark.read.option("header", True).csv(str(DATA / "geo_deliveries" / "stores.csv"))
store_cities = stores.select(F.col("city").alias("store_city")).distinct()
joined = deliveries.join(store_cities, F.collate(F.trim("city"), "UNICODE_CI_AI") ==
                         F.collate("store_city", "UNICODE_CI_AI"), "left")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")     # show the shuffle join
print([l.strip()[:110] for l in plan(joined, "simple").splitlines() if "Join" in l or "hashpartitioning" in l][:2])
spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
print("matched:", joined.where("store_city IS NOT NULL").count(), "of", deliveries.count())

# 6. UTF-8 validation.
print("\n== 6. invalid UTF-8")
bad = (spark.createDataFrame([(bytearray(b"Caf\xe9"),), (bytearray("Café".encode()),)], "b BINARY")
       .select(F.col("b").cast("string").alias("s")))
bad.select(F.hex(F.col("s").cast("binary")).alias("bytes"), F.is_valid_utf8("s").alias("valid"),
           F.make_valid_utf8("s").alias("made_valid"), F.try_validate_utf8("s").alias("try_valid")).show()
print("validate_utf8:", attempt(lambda: bad.select(F.validate_utf8("s")).collect(), 70))
shutil.rmtree(tmp)

# 7. Pattern matching: LIKE, ILIKE, RLIKE and regexp_extract.
print("\n== 7. LIKE, ILIKE, RLIKE")
inv = spark.createDataFrame([("INV-2026-001",), ("inv_2026_002",), ("INV-26-3",), ("INVX2026",)], "s STRING")
inv.select("s", F.col("s").like("INV-%").alias("like"), F.col("s").ilike("inv%").alias("ilike"),
           F.expr(r"s LIKE 'inv\\_%'").alias("like_esc"), F.expr("s LIKE 'INV_2026%'").alias("like_any1"),
           F.col("s").rlike(r"^INV-\d{4}-\d{3}$").alias("rlike"),
           F.regexp_extract("s", r"(\d{4})", 1).alias("year")).show()
print(spark.sql(r"SELECT 'a.b' RLIKE 'a.b' AS dot_any, 'axb' RLIKE 'a.b' AS dot_x, 'axb' RLIKE 'a\\.b' AS escaped, "
                r"'xxINVxx' RLIKE 'INV' AS rlike_finds_inside, 'xxINVxx' LIKE 'INV' AS like_whole").first())
print(spark.sql(r"SELECT regexp_extract('INV-26-3', '([0-9]{4})', 1) AS extract, "
                r"regexp_substr('INV-26-3', '[0-9]{4}') AS substr").first())

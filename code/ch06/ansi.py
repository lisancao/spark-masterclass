"""Chapter 6: ANSI mode, the errors it raises, and the try_ functions that return NULL instead.

Run from code/:  uv run python ch06/ansi.py
"""
import re
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.errors import PySparkException
from pyspark.sql import functions as F
from common.book import session, TRIPS

CLEANING = Path(__file__).resolve().parents[1] / "data" / "cleaning"
spark = session("ch06-ansi", spark_sql_session_timeZone="UTC")


def error_line(e):
    for line in str(e).splitlines():
        m = re.search(r"\[[A-Z_.]+\].*", line)
        if m:
            return m.group(0)
    return str(e).splitlines()[0]


def attempt(f, width=84):
    try:
        return f()
    except Exception as e:
        return "ERROR " + error_line(e)[:width]


# 1. On real data: Chapter 3's tip ratio, without the cleaning filter in front of it.
print("== 1. tip / fare over every June trip")
trips = spark.read.parquet(str(TRIPS))
ratio = trips.select((F.col("tip_amount") / F.col("fare_amount")).alias("r"))
print("defined the DataFrame:", ratio.columns)
try:
    ratio.agg(F.avg("r")).collect()
except PySparkException as e:
    print("avg(tip / fare):", e.getCondition(), "SQLSTATE", e.getSqlState())
print("trips with fare_amount = 0:", trips.where("fare_amount = 0").count())
r = trips.agg(F.avg(F.try_divide("tip_amount", "fare_amount")).alias("avg_ratio"),
              F.count(F.try_divide("tip_amount", "fare_amount")).alias("ratios")).first()
print(f"avg(try_divide(tip, fare)) = {r.avg_ratio:.4f} over {r.ratios:,} trips")

# 2. The errors ANSI mode raises, and what the same queries return with ANSI off.
print("\n== 2. ANSI on and off")
queries = ["SELECT 10 / 0", "SELECT 10.0D / 0", "SELECT 7 % 0", "SELECT 2147483647 + 1",
           "SELECT CAST(3000000000 AS INT)", "SELECT CAST('12a' AS INT)",
           "SELECT element_at(array(1, 2), 5)", "SELECT array(1, 2)[5]",
           "SELECT to_date('2026-06-04', 'dd/MM/yyyy')", "SELECT CAST('2026-02-30' AS DATE)"]
print(f"  {'query':<44} {'ANSI on':<36} ANSI off")
for q in queries:
    spark.conf.set("spark.sql.ansi.enabled", "true")
    on = attempt(lambda: spark.sql(q).first()[0])
    on = re.sub(r"^ERROR \[([A-Z_.]+)\].*", r"\1", str(on))
    spark.conf.set("spark.sql.ansi.enabled", "false")
    off = attempt(lambda: spark.sql(q).first()[0])
    print(f"  {q:<44} {on:<36} {off}")
spark.conf.set("spark.sql.ansi.enabled", "true")

# 3. The try_ family.
print("\n== 3. try_ functions")
spark.sql("""SELECT try_divide(10, 0) AS a, try_add(2147483647, 1) AS b, try_cast('12a' AS INT) AS c,
             '12'::INT + 1 AS d, try_element_at(array(1, 2), 5) AS e, try_mod(7, 0) AS f,
             try_to_date('2026-02-30') AS g, try_to_timestamp('noon') AS h,
             try_to_number('$1,234.50', '$9,999.99') AS i, try_parse_url('not a url', 'HOST') AS j""").show()
fns = sorted(r.function for r in spark.sql("SHOW FUNCTIONS LIKE 'try_*'").collect())
print(f"{len(fns)} try_ functions in 4.3.0:")
for i in range(0, len(fns), 5):
    print("  " + "  ".join(f"{f:<22}" for f in fns[i:i + 5]).rstrip())

# 4. What a cast from text accepts.
print("\n== 4. what a cast accepts")
vals = spark.createDataFrame([(v,) for v in [" 12 ", "+12", "0012", "12.7", "1e3", "1,234", " 12", "\t12\n"]],
                             "s STRING")
vals.select(F.to_json(F.struct("s")).alias("s"), F.col("s").try_cast("int").alias("as_int"),
            F.col("s").try_cast("double").alias("as_double"),
            F.col("s").try_cast("decimal(10,2)").alias("as_decimal")).show()

print("== 5. booleans")
spark.sql("""SELECT x, try_cast(x AS BOOLEAN) AS b FROM VALUES ('yes'), ('Y'), ('t'), ('1'), (' TRUE '),
             ('no'), ('n'), ('0'), ('false'), ('on'), ('maybe') AS v(x)""").show()

# 6. The error appears at the action, and comes with a condition and the offending value.
print("== 6. where the error appears")
raw = spark.read.option("header", True).csv(str(CLEANING / "customers_raw.csv"))
ages = raw.select("customer_id", F.col("age").cast("int").alias("age"))
print("defined:", ages.columns)
try:
    ages.collect()
except PySparkException as e:
    print("collect():", e.getCondition(), e.getSqlState())
    print("  ", error_line(e)[:110])
    print("  parameters:", e.getMessageParameters())
raw.select("customer_id", "age", F.col("age").try_cast("int").alias("age_int")).show(8)

# 7. &, | and ~ need parentheses.
print("== 7. conditions")
print(attempt(lambda: raw.filter(F.col("age") > 30 & F.col("city") == "Paris"), 110))
print("parenthesized:", raw.filter((F.col("age").try_cast("int") > 30) & (F.col("city") == "Paris")).count())

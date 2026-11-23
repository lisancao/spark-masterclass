"""Chapter 10: money, dates, timestamps, NULL markers, NaN, mapping tables and reject reasons,
ending with a full cleanup of SparkTutor's customer export.

Run from code/:  uv run python ch10/values.py
"""
import re
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, plan

CLEANING = Path(__file__).resolve().parents[1] / "data" / "cleaning"
spark = session("ch10-values", spark_sql_session_timeZone="UTC")


def attempt(f, width=90):
    try:
        return f()
    except Exception as e:
        for line in str(e).splitlines():
            m = re.search(r"\[[A-Z_.]+\].*", line)
            if m:
                return "ERROR " + m.group(0)[:width]
        return "ERROR " + str(e).splitlines()[0][:width]


# 1. Money.
print("== 1. money")
money = spark.createDataFrame([(v,) for v in ["$1,234.50", "$12", "1234.5", "-$5.00", " $7.25 ", "1.234,50", "abc"]],
                              "s STRING")
money.select("s", F.try_to_number("s", F.lit("$9,999.99")).alias("to_number"),
             F.regexp_replace("s", r"[$,\s]", "").try_cast("decimal(12,2)").alias("stripped"),
             F.regexp_replace(F.regexp_replace("s", r"\.", ""), ",", ".").try_cast("decimal(12,2)").alias("eu")).show()
print(spark.sql("SELECT to_char(1234.5, '$9,999.99') AS to_char, format_number(1234567.891, 2) AS fmt, "
                "try_to_number('5.00-', '999.99MI') AS mi, try_to_number('<5.00>', '999.99PR') AS pr").first())

# 2. Dates in many formats.
print("\n== 2. dates")
DATE_PATTERNS = ["yyyy/MM/dd", "dd/MM/yyyy", "d MMM yyyy", "MMMM d, yyyy", "yyyyMMdd"]


def parse_date(c):
    """The first pattern that parses wins; NULL if none does."""
    return F.coalesce(F.try_to_date(c), *[F.try_to_date(c, p) for p in DATE_PATTERNS])


dates = spark.createDataFrame([(v,) for v in ["2025-03-04", "2025/03/05", "05/03/2025", "4 Mar 2025",
                                               "March 4, 2025", "20250306", "2025-03-09T14:30:00",
                                               "2025-02-30", "unknown"]], "s STRING")
dates.select("s", F.try_to_date("s").alias("default_only"), parse_date(F.col("s")).alias("parsed")).show(truncate=False)
spark.sql("""SELECT try_to_date('04/03/2026', 'dd/MM/yyyy') AS day_first,
  try_to_date('04/03/2026', 'MM/dd/yyyy') AS month_first,
  try_to_date('2026-03-04', 'yyyy-mm-dd') AS minutes_trap,
  try_to_date('4/3/2026', 'dd/MM/yyyy') AS dd_needs_two,
  try_to_date('4/3/2026', 'd/M/yyyy') AS d_takes_both,
  try_to_date('04-Mar-69', 'dd-MMM-yy') AS yy_69""").show()
print("YYYY:", attempt(lambda: spark.sql("SELECT try_to_date('2026-03-04', 'YYYY-MM-dd')").first()))

# 3. Timestamps: text with and without an offset.
print("\n== 3. timestamps")
q = """SELECT date_format(try_to_timestamp('2026-03-04 09:30'), 'yyyy-MM-dd HH:mm XXX') AS no_offset,
  date_format(try_to_timestamp('2026-03-04T09:30:00-05:00'), 'yyyy-MM-dd HH:mm XXX') AS with_offset,
  cast(to_timestamp_ntz('2026-03-04 09:30') AS string) AS ntz"""
for tz in ["UTC", "America/Toronto"]:
    spark.conf.set("spark.sql.session.timeZone", tz)
    print(f"{tz:<16}", spark.sql(q).first())
spark.conf.set("spark.sql.session.timeZone", "UTC")
print(spark.sql("SELECT cast(timestamp_seconds(1772616600) AS string) AS from_epoch").first())

# 4. NULL, "", "N/A" and NaN.
print("\n== 4. NULL, empty, markers, NaN")
v = spark.createDataFrame([("a", None, float("nan")), ("b", "", 1.0), ("c", "N/A", None), ("d", "Paris", 2.0)],
                          "k STRING, city STRING, score DOUBLE")
v.show()
print("city != 'Paris':", v.filter(F.col("city") != "Paris").count(),
      "| city == None:", v.filter(F.col("city") == None).count(),        # noqa: E711, the trap itself
      "| isNull:", v.filter(F.col("city").isNull()).count())
spark.sql("""SELECT NULL = NULL AS eq, NULL <=> NULL AS null_safe, equal_null(NULL, NULL) AS fn,
  'x' NOT IN ('a', NULL) AS not_in, concat('a', NULL) AS concat, concat_ws('-', 'a', NULL, 'b') AS concat_ws""").show()
v.select("k", "city", F.coalesce("city", F.lit("unknown")).alias("coalesce"),
         F.nullif(F.col("city"), F.lit("")).alias("nullif_empty"),
         F.when(F.upper(F.trim("city")).isin("", "N/A", "NA", "NULL"), None).otherwise(F.col("city"))
          .alias("markers_to_null")).show()
print("coalesce(date, 'n/a'):", attempt(lambda: spark.sql(
    "SELECT coalesce(try_to_date('x'), 'n/a')").first(), 80))
v.select("k", "score", F.isnan("score").alias("isnan"), F.col("score").isNull().alias("isnull"),
         F.nanvl("score", F.lit(None)).alias("nan_to_null")).show()
print("NaN = NaN:", spark.sql("SELECT double('NaN') = double('NaN')").first()[0],
      "| sorted:", [r.score for r in v.orderBy("score").collect()])
print("aggregates with NaN:   ", v.agg(F.count("score"), F.avg("score"), F.max("score")).first())
print("after nanvl:           ", v.select(F.nanvl("score", F.lit(None)).alias("s"))
      .agg(F.count("s"), F.avg("s"), F.max("s")).first())
print("fillna(0.0):", [r.score for r in v.fillna({"score": 0.0}).collect()])

# 5. The full cleanup of customers_raw.csv.
print("\n== 5. clean the customer export")
NULL_TOKENS = ["", "N/A", "NA", "NULL", "NONE", "-"]
EMAIL_RE = r"^[^@\s]+@[^@\s]+\.[a-z]{2,}$"
REASONS = ["missing_name", "bad_email", "bad_phone", "bad_country", "bad_date", "bad_ltv", "bad_newsletter", "bad_age"]


def tidy(c):
    """Every run of whitespace (with U+00A0) to one space, trimmed, NULL markers to NULL."""
    squeezed = F.trim(F.regexp_replace(c, r"[\s ]+", " "))
    return F.when(F.upper(squeezed).isin(NULL_TOKENS), None).otherwise(squeezed)


def parse_money(c):
    return F.regexp_replace(c, r"[$,\s]", "").try_cast("decimal(12,2)")


def clean_customers(raw, countries):
    t = raw.withColumns({c: tidy(F.col(c)) for c in raw.columns})
    digits = F.regexp_replace("phone", r"[^0-9]", "")
    phone10 = F.when((F.length(digits) == 11) & digits.startswith("1"), F.substring(digits, 2, 10)).otherwise(digits)
    email, age = F.lower("email"), F.col("age").try_cast("int")
    typed = t.withColumns({
        "customer_id": F.upper("customer_id"), "name": F.initcap("full_name"),
        "email_ok": email.rlike(EMAIL_RE), "email_clean": email,
        "phone_ok": phone10.rlike(r"^[0-9]{10}$"), "phone_clean": phone10,
        "country_key": F.upper(F.regexp_replace("country", r"\.", "")),
        "signup": parse_date(F.col("signup_date")), "ltv": parse_money(F.col("lifetime_value")),
        "news": F.col("newsletter").try_cast("boolean"), "age_int": age, "age_ok": age.between(0, 120)})
    mapped = typed.join(F.broadcast(countries), F.col("country_key") == F.col("raw"), "left")
    checks = {
        "missing_name": F.col("name").isNull(),
        "bad_email": F.col("email").isNotNull() & ~F.col("email_ok"),
        "bad_phone": F.col("phone").isNotNull() & ~F.col("phone_ok"),
        "bad_country": F.col("country").isNotNull() & F.col("code").isNull(),
        "bad_date": F.col("signup_date").isNotNull() & F.col("signup").isNull(),
        "bad_ltv": F.col("lifetime_value").isNotNull() & F.col("ltv").isNull(),
        "bad_newsletter": F.col("newsletter").isNotNull() & F.col("news").isNull(),
        "bad_age": F.col("age").isNotNull() & ~F.coalesce(F.col("age_ok"), F.lit(False))}
    reasons = F.concat_ws(",", *[F.when(checks[r], F.lit(r)) for r in REASONS])
    return mapped.select(
        "customer_id", "name",
        F.when(F.col("email_ok"), F.col("email_clean")).alias("email"),
        F.when(F.col("phone_ok"), F.col("phone_clean")).alias("phone"),
        F.col("code").alias("country"), F.col("signup").alias("signup_date"),
        F.col("ltv").alias("lifetime_value"), F.col("news").alias("newsletter"),
        F.when(F.col("age_ok"), F.col("age_int")).alias("age"), "city",
        F.nullif(reasons, F.lit("")).alias("reject_reason"))


raw = spark.read.option("header", True).csv(str(CLEANING / "customers_raw.csv"))
countries = spark.read.option("header", True).csv(str(CLEANING / "country_codes.csv"))
print("raw:", raw.count(), "rows, all strings:", {t for _, t in raw.dtypes})
raw.select("customer_id", "full_name", "signup_date", "lifetime_value", "newsletter", "age", "country") \
   .where("trim(upper(customer_id)) IN ('C001', 'C002', 'C003', 'C004', 'C006')").show(truncate=False)
clean = clean_customers(raw, countries)
clean.printSchema()
(clean.select("customer_id", "name", "country", "signup_date", "lifetime_value", "newsletter", "age", "reject_reason")
 .where("customer_id IN ('C001', 'C002', 'C003', 'C004', 'C006')").orderBy("customer_id").show(truncate=False))
print("rows kept:", clean.count(), "| rows with a reject reason:", clean.where("reject_reason IS NOT NULL").count())
(clean.select(F.explode(F.split("reject_reason", ",")).alias("reason")).groupBy("reason").count()
 .orderBy(F.desc("count"), "reason").show())
print("unmapped countries:", [r.country for r in raw.join(F.broadcast(countries),
      F.upper(F.regexp_replace(F.trim("country"), r"\.", "")) == F.col("raw"), "left_anti")
      .where("country IS NOT NULL").select("country").distinct().collect()])
(clean.groupBy(F.collate("city", "UNICODE_CI_AI").alias("city")).count().orderBy(F.desc("count"), "city").show(5))
print("\nthe plan of the cleanup: one Project per side of one broadcast join")
print("\n".join(l[:100] for l in plan(clean, "simple").splitlines() if l.strip()))

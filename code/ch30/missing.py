"""Chapter 30: what happens when the Avro and Protobuf packages are not on the classpath.

Run from code/:  uv run python ch30/missing.py
"""
import re
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.book import session, MESSY

spark = session("ch30-missing")


def attempt(label, fn):
    try:
        print(f"  {label:42} -> {fn()}")
    except Exception as e:
        m = re.search(r"\[([A-Z_0-9.]+)\]", str(e))
        print(f"  {label:42} -> {m.group(1) if m else type(e).__name__}")


print("== no --packages ==")
attempt("spark.read.format('avro')", lambda: spark.read.format("avro").load("/tmp").count())
attempt("SQL from_avro", lambda: spark.sql("SELECT from_avro(X'00', '\"int\"', map())").first())
attempt("SQL from_protobuf", lambda: spark.sql(
    f"SELECT from_protobuf(X'00', 'shop.Payment', '{MESSY}/binary/schemas/payments.desc')").first())
from pyspark.sql.avro.functions import from_avro  # noqa: E402
attempt("Python from_avro", lambda: spark.range(1).select(from_avro("id", '"int"')).first())

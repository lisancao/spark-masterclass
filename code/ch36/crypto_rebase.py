"""Chapter 36: Parquet modular (columnar) encryption, and the datetime rebase modes for old files.

Encryption uses the mock InMemoryKMS from parquet-hadoop-1.17.0-tests.jar (Parquet 1.17.0 is the version
in Spark 4.3.0), a download into $BOOK_DATA/jars/dl-parquet/. The mock KMS is for demonstrations only.
Run from code/:  uv run python ch36/crypto_rebase.py [crypto] [rebase]
"""
import datetime as dt, shutil, struct, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pyarrow as pa, pyarrow.parquet as pq
from common.book import session, TRIPS, DATA

WANT = set(sys.argv[1:]) or {"crypto", "rebase"}
TESTS_JAR = DATA / "jars" / "dl-parquet" / "parquet-hadoop-1.17.0-tests.jar"
OUT = Path("/tmp/masterclass-ch36-crypto")
shutil.rmtree(OUT, ignore_errors=True); OUT.mkdir(parents=True)
spark = session("ch36-crypto", spark_sql_session_timeZone="UTC", spark_ui_showConsoleProgress="false",
                spark_jars=str(TESTS_JAR))
hconf = spark.sparkContext._jsc.hadoopConfiguration()
FACTORY = "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory"


def keys_on():
    hconf.set("parquet.crypto.factory.class", FACTORY)
    hconf.set("parquet.encryption.kms.client.class", "org.apache.parquet.crypto.keytools.mocks.InMemoryKMS")
    hconf.set("parquet.encryption.key.list", "keyA:AAECAwQFBgcICQoLDA0ODw==, keyB:AAECAAECAAECAAECAAECAA==")


def keys_off():
    for k in ("parquet.crypto.factory.class", "parquet.encryption.kms.client.class", "parquet.encryption.key.list"):
        hconf.unset(k)


def err(e, n=150):
    msg = " ".join(str(e).split())
    root = msg.split("Caused by: ")[-1] if "Caused by: " in msg else msg
    return f"{type(e).__name__}: {root[:n]}"


def tail(path):
    data = Path(path).read_bytes()
    return data[:4], data[-4:], struct.unpack("<I", data[-8:-4])[0], len(data)


def one(path):
    return next(Path(path).glob("*.parquet"))


if "crypto" in WANT:
    print("=== 1. columnar encryption: two columns with keyA, the footer with keyB")
    trips = spark.read.parquet(str(TRIPS)).select("PULocationID", "trip_distance", "fare_amount", "tip_amount").limit(100_000)
    keys_on()
    for name, extra in [("encrypted_footer", {}), ("plaintext_footer", {"parquet.encryption.plaintext.footer": "true"})]:
        w = (trips.coalesce(1).write.option("parquet.encryption.column.keys", "keyA:fare_amount,tip_amount")
             .option("parquet.encryption.footer.key", "keyB"))
        for k, v in extra.items():
            w = w.option(k, v)
        w.parquet(str(OUT / name))
        head, magic, flen, size = tail(one(OUT / name))
        print(f"  {name:17} {size:>9,} bytes  first 4 bytes {head}  last 4 bytes {magic}  footer length {flen:,}")
    trips.coalesce(1).write.parquet(str(OUT / "plain"))
    head, magic, flen, size = tail(one(OUT / "plain"))
    print(f"  {'not encrypted':17} {size:>9,} bytes  first 4 bytes {head}  last 4 bytes {magic}  footer length {flen:,}")

    print("\n=== 2. read with the keys (the crypto factory and the KMS are set)")
    for name in ("encrypted_footer", "plaintext_footer"):
        r = spark.read.parquet(str(OUT / name)).selectExpr("count(*) n", "round(sum(tip_amount), 2) tips").first()
        print(f"  {name:17} rows {r.n:,}, sum(tip_amount) {r.tips}")

    print("\n=== 3. read without the keys")
    keys_off()
    for name in ("encrypted_footer", "plaintext_footer"):
        for cols in (["PULocationID", "trip_distance"], ["tip_amount"]):
            try:
                r = spark.read.parquet(str(OUT / name)).selectExpr(*[f"max({c})" for c in cols]).first()
                print(f"  {name:17} {', '.join(cols):28} max: {', '.join(str(v) for v in r)}")
            except Exception as e:
                print(f"  {name:17} {', '.join(cols):28} {err(e, 110)}")
    for name in ("encrypted_footer", "plaintext_footer"):
        try:
            md = pq.ParquetFile(one(OUT / name)).metadata
            print(f"  pyarrow, {name:17} footer: {md.num_rows:,} rows, {md.num_columns} columns, created_by {md.created_by}")
            t = pq.read_table(one(OUT / name), columns=["trip_distance"])
            print(f"  pyarrow, {name:17} trip_distance: {t.num_rows:,} rows")
            t = pq.read_table(one(OUT / name), columns=["tip_amount"])
        except Exception as e:
            print(f"  pyarrow, {name:17} {err(e, 110)}")

if "rebase" in WANT:
    print("\n=== 4. dates before 1582: the hybrid calendar and the proleptic Gregorian calendar")
    print("  defaults in 4.3.0: " + ", ".join(f"{k.split('.')[-1]}={spark.conf.get(k)}" for k in (
        "spark.sql.parquet.datetimeRebaseModeInRead", "spark.sql.parquet.datetimeRebaseModeInWrite",
        "spark.sql.parquet.int96RebaseModeInRead", "spark.sql.parquet.int96RebaseModeInWrite")))
    df = spark.sql("SELECT DATE'1000-01-01' AS d, TIMESTAMP'1000-01-01 12:00:00' AS ts, DATE'2026-06-01' AS recent")
    for mode in ("CORRECTED", "LEGACY"):
        spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", mode)
        spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        df.coalesce(1).write.parquet(str(OUT / f"spark_{mode.lower()}"))
        f = one(OUT / f"spark_{mode.lower()}")
        kv = pq.ParquetFile(f).metadata.metadata
        legacy_key = b"org.apache.spark.legacyDateTime" in kv
        raw = pq.read_table(f).to_pylist()[0]
        days = pq.read_table(f).column("d").cast(pa.int32())[0].as_py()
        print(f"  Spark writes with {mode:9}: stored day number {days:>8}, pyarrow reads d={raw['d']}, "
              f"legacyDateTime key: {legacy_key}")
    spark.conf.unset("spark.sql.parquet.datetimeRebaseModeInWrite")

    print("\n  Spark reads its own files back (the key in the footer decides, not the read mode):")
    for mode in ("CORRECTED", "LEGACY"):
        for read_mode in ("CORRECTED", "LEGACY", "EXCEPTION"):
            spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", read_mode)
            r = spark.read.parquet(str(OUT / f"spark_{mode.lower()}")).selectExpr("string(d) d", "string(ts) ts").first()
            print(f"    file {mode:9} read mode {read_mode:9} -> d={r.d}  ts={r.ts}")

    print("\n  a file from another writer (pyarrow, no Spark keys in the footer), with the same day number")
    legacy_days = pq.read_table(one(OUT / "spark_legacy")).column("d").cast(pa.int32())[0].as_py()
    pq.write_table(pa.table({"d": pa.array([legacy_days], pa.int32()).cast(pa.date32())}), OUT / "other.parquet")
    for read_mode in ("CORRECTED", "LEGACY", "EXCEPTION"):
        spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", read_mode)
        try:
            print(f"    read mode {read_mode:9} -> d={spark.read.parquet(str(OUT / 'other.parquet')).selectExpr('string(d) d').first().d}")
        except Exception as e:
            print(f"    read mode {read_mode:9} -> {err(e, 120)}")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    r = spark.read.option("datetimeRebaseMode", "LEGACY").parquet(str(OUT / "other.parquet")).selectExpr("string(d) d").first()
    print(f"    read option datetimeRebaseMode=LEGACY -> d={r.d}")
    spark.conf.unset("spark.sql.parquet.datetimeRebaseModeInRead")

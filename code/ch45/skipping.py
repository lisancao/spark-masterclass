"""Chapter 45: how a Parquet layout decides what a filter can skip, measured as rows the scan returned.

A deterministic table of 2,000,000 events, written in several layouts. Each query's scan reports
`number of output rows` in the SQL tab; that metric counts the rows that left the Parquet reader.
Classic (SQL REST metrics). Run from code/:  uv run python ch45/skipping.py [section ...]
Sections: layouts timestamps codecs v2 nanos
"""
import shutil, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pyarrow.parquet as pq
from pyspark.sql import functions as F
from common.book import session, plan, ui_api, TRIPS

WANT = set(sys.argv[1:]) or {"layouts", "timestamps", "codecs", "v2", "nanos"}
spark = session("ch45-skipping", spark_sql_session_timeZone="UTC", spark_driver_memory="4g",
                spark_ui_showConsoleProgress="false")
OUT = Path("/tmp/masterclass-ch45")
N = 2_000_000

# user_id: 20,000 users; event: 6 kinds, refund only for 100 users; session_id: random 16 hex chars; ts: one day per 200,000 rows
events = (spark.range(N, numPartitions=8)
          .select(F.col("id"),
                  ((F.col("id") * 7919) % 20000).alias("user_id"),
                  F.when((((F.col("id") * 7919) % 20000) < 100) & (F.col("id") % 2 == 0), "refund")
                   .otherwise(F.element_at(F.array(*[F.lit(e) for e in ["view", "click", "cart", "buy", "search"]]),
                                           (F.col("id") % 5 + 1).cast("int"))).alias("event"),
                  F.substring(F.sha2(F.col("id").cast("string"), 256), 1, 16).alias("session_id"),
                  (F.lit("2026-06-01").cast("timestamp") + F.make_interval(secs=F.col("id") * 0.432)).alias("ts"),
                  (F.col("id") % 10000 / 100).alias("amount")))
probe = events.where("id = 1234567").first()


def rows_scanned(df, label):
    """Run df, then read the scan node's `number of output rows` from the SQL tab."""
    spark.sparkContext.setJobDescription(label)
    df.collect()
    spark.sparkContext.setJobDescription(None)
    q = [x for x in ui_api("sql?details=true&planDescription=false") if x["description"] == label][-1]
    for n in q["nodes"]:
        if n["nodeName"].startswith("Scan"):
            m = {x["name"]: x["value"] for x in n["metrics"]}
            return int(m["number of output rows"].replace(",", "")), m.get("number of files read")
    return None, None


def write(df, name, **opts):
    path = OUT / name
    shutil.rmtree(path, ignore_errors=True)
    w = df.write
    for k, v in opts.items():
        w = w.option(k, v)
    w.parquet(str(path))
    files = list(path.glob("*.parquet"))
    groups = sum(pq.ParquetFile(f).metadata.num_row_groups for f in files)
    return path, len(files), groups, sum(f.stat().st_size for f in files) / 2**20


if "layouts" in WANT:
    print(f"{N:,} events; probe row: user_id {probe.user_id}, session_id {probe.session_id}\n")
    LAYOUTS = [
        ("A: as generated, 8 files", events, {}),
        ("B: sorted by user_id, 8 files", events.repartitionByRange(8, "user_id").sortWithinPartitions("user_id"), {}),
        ("C: B + 1 MB row groups", events.repartitionByRange(8, "user_id").sortWithinPartitions("user_id"),
         {"parquet.block.size": 1024 * 1024}),
        ("D: C + bloom filter on session_id", events.repartitionByRange(8, "user_id").sortWithinPartitions("user_id"),
         {"parquet.block.size": 1024 * 1024, "parquet.bloom.filter.enabled#session_id": "true",
          "parquet.bloom.filter.expected.ndv#session_id": "20000"}),
    ]
    QUERIES = [
        ("user_id = probe", lambda d: d.where(F.col("user_id") == probe.user_id)),
        ("event = 'refund'", lambda d: d.where(F.col("event") == "refund")),
        ("session_id = probe", lambda d: d.where(F.col("session_id") == probe.session_id)),
        ("ts in one hour", lambda d: d.where(F.col("ts").between("2026-06-03 10:00", "2026-06-03 11:00"))),
    ]
    print(f"{'layout':<36}{'files':>6}{'row groups':>11}{'MB':>7}")
    paths = []
    for name, df, opts in LAYOUTS:
        path, files, groups, mb = write(df, name[0], **opts)
        paths.append(path)
        print(f"{name:<36}{files:>6}{groups:>11}{mb:>7.1f}")
    print(f"\nrows that the Parquet reader returned (of {N:,}):")
    print(f"{'query':<22}" + "".join(f"{n[0]:>12}" for n, _, _ in LAYOUTS))
    for qname, q in QUERIES:
        cells = [rows_scanned(q(spark.read.parquet(str(p))), f"{qname} {p.name}")[0] for p in paths]
        print(f"{qname:<22}" + "".join(f"{c:>12,}" for c in cells))
    df = QUERIES[0][1](spark.read.parquet(str(paths[2])))
    print("\nPushedFilters for user_id = probe:", next(l.split("PushedFilters: ")[1].split("]")[0] + "]"
                                                    for l in plan(df, "formatted").splitlines() if "PushedFilters" in l))
    md = pq.ParquetFile(next(paths[2].glob("*.parquet"))).metadata
    rg = md.row_group(0).column(1)
    print(f"layout C, first file, row group 0: {md.row_group(0).num_rows:,} rows, user_id min {rg.statistics.min} "
          f"max {rg.statistics.max}; {md.num_row_groups} row groups in the file")

    # Turn off one mechanism at a time on layout D.
    print("\nlayout D with one reader feature off (rows returned):")
    OFFS = [("all on (defaults)", {}),
            ("spark.sql.parquet.filterPushdown=false", {"spark.sql.parquet.filterPushdown": "false"}),
            ("stats off", {"parquet.filter.stats.enabled": "false"}),
            ("column index off", {"parquet.filter.columnindex.enabled": "false"}),
            ("stats and column index off", {"parquet.filter.stats.enabled": "false",
                                            "parquet.filter.columnindex.enabled": "false"}),
            ("stats, column index and dictionary off", {"parquet.filter.stats.enabled": "false",
                                                        "parquet.filter.columnindex.enabled": "false",
                                                        "parquet.filter.dictionary.enabled": "false"}),
            ("dictionary off", {"parquet.filter.dictionary.enabled": "false"}),
            ("bloom filter off", {"parquet.filter.bloom.enabled": "false"})]
    print(f"{'setting':<44}" + "".join(f"{q[0][:11]:>13}" for q in QUERIES))
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    for label, conf in OFFS:
        for k, v in conf.items():
            (spark.conf.set if k.startswith("spark.") else hconf.set)(k, v)
        cells = [rows_scanned(q(spark.read.parquet(str(paths[3]))), f"off {label} {qn}")[0] for qn, q in QUERIES]
        print(f"{label:<44}" + "".join(f"{c:>13,}" for c in cells))
        for k in conf:
            spark.conf.unset(k) if k.startswith("spark.") else hconf.unset(k)

if "timestamps" in WANT:
    print("\n=== timestamps: the physical type decides if a filter can skip")
    small = events.repartitionByRange(4, "ts").sortWithinPartitions("ts")
    for t in ("INT96", "TIMESTAMP_MICROS"):
        spark.conf.set("spark.sql.parquet.outputTimestampType", t)
        path, files, groups, mb = write(small, f"ts_{t}", **{"parquet.block.size": 1024 * 1024})
        f = next(path.glob("*.parquet"))
        col = pq.ParquetFile(f).metadata.row_group(0).column(4)
        df = spark.read.parquet(str(path)).where(F.col("ts").between("2026-06-03 10:00", "2026-06-03 11:00"))
        n, _ = rows_scanned(df, f"ts {t}")
        pushed = "GreaterThanOrEqual(ts" in plan(df, "formatted")
        print(f"  {t:<18} physical {col.physical_type:<6} stats {'yes' if col.is_stats_set and col.statistics.has_min_max else 'no ':<4}"
              f" ts filter in PushedFilters: {pushed!s:<6} rows returned {n:>9,}")
    spark.conf.unset("spark.sql.parquet.outputTimestampType")
    print("  default spark.sql.parquet.outputTimestampType:", spark.conf.get("spark.sql.parquet.outputTimestampType"))

if "codecs" in WANT:
    print("\n=== compression codecs on the June trips (one file each)")
    june = spark.read.parquet(str(TRIPS))
    import time
    for codec in ("none", "snappy", "lz4_raw", "gzip", "zstd"):
        t = time.perf_counter()
        path, files, groups, mb = write(june.coalesce(1), f"codec_{codec}", compression=codec)
        w = time.perf_counter() - t
        t = time.perf_counter()
        spark.read.parquet(str(path)).write.format("noop").mode("overwrite").save()
        r = time.perf_counter() - t
        print(f"  {codec:<8}{mb:>8.1f} MB  {groups} row groups  write {w:5.1f} s  full read {r:5.2f} s")
    print("  default spark.sql.parquet.compression.codec:", spark.conf.get("spark.sql.parquet.compression.codec"))

if "v2" in WANT:
    print("\n=== the v1 and v2 page formats")
    from thrift_compact import Compact, footer_bytes, page_headers, ENCODINGS
    sample = events.limit(200_000).coalesce(1)
    for version in ("v1", "v2"):
        path, files, groups, mb = write(sample, f"pages_{version}", **{"parquet.writer.version": version})
        f = next(path.glob("*.parquet"))
        meta = Compact(footer_bytes(f)).struct()
        print(f"  {version}: {mb:.2f} MB")
        for chunk in meta[4][0][1]:
            md = chunk[3]
            pages = page_headers(f, md)
            kinds = sorted({(t, e) for t, e, _, _ in pages})
            print(f"    {md[3][0].decode():<11}{len(pages):>3} pages  {', '.join(f'{t} {e}' for t, e in kinds)}")

if "nanos" in WANT:
    print("\n=== nanosecond timestamps (4.3 preview)")
    import re
    spark.conf.set("spark.sql.timestampNanosTypes.enabled", "true")
    df = spark.sql("SELECT CAST('2026-06-01 08:00:00.123456789' AS TIMESTAMP_NTZ(9)) AS t")
    print("  schema in the JVM:", df._jdf.schema().simpleString(), "| as a string:", df.selectExpr("CAST(t AS STRING)").first()[0])
    try:
        df.schema
    except Exception as e:
        print("  df.schema in classic PySpark ->", (re.findall(r"\[([A-Z][A-Z_.]+)\]", str(e)) or [str(e)[:80]])[0])
    path, *_ = write(df.coalesce(1), "nanos")
    col = pq.ParquetFile(next(path.glob("*.parquet"))).schema.column(0)
    print(f"  written as: {col.physical_type}, {col.logical_type}")
    back = spark.read.parquet(str(path))
    print("  read back:", back._jdf.schema().simpleString(), back.selectExpr("CAST(t AS STRING)").first()[0])
    spark.conf.unset("spark.sql.timestampNanosTypes.enabled")

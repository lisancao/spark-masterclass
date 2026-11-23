"""Chapter 45: ORC from the inside, and ORC against Parquet on the June trips.

Sections: dump (file structure with orc-tools), readers (native and hive), compare (pushdown and
vectorized reads, timings under the bench lock). orc-tools 2.3.0 (the ORC version of Spark 4.3.0) is a
download: $BOOK_DATA/jars/dl-orc/orc-tools-2.3.0-uber.jar (sha256 in notes/v3-perf.md).
Run from code/:  uv run python ch45/orc.py [dump] [readers] [compare]
"""
import contextlib, fcntl, json, os, re, shutil, statistics, subprocess, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, plan, ui_api, TRIPS, ZONES, DATA

WANT = set(sys.argv[1:]) or {"dump", "readers", "compare"}
ORC_TOOLS = DATA / "jars" / "dl-orc" / "orc-tools-2.3.0-uber.jar"
OUT = Path("/tmp/masterclass-ch45-orc")
spark = session("ch45-orc", spark_sql_session_timeZone="UTC", spark_driver_memory="4g",
                spark_ui_showConsoleProgress="false")


def tools(*args):
    r = subprocess.run(["java", "-jar", str(ORC_TOOLS), *args], capture_output=True, text=True)
    return r.stdout


def one_file(df, name, fmt="orc", **opts):
    path = OUT / name
    shutil.rmtree(path, ignore_errors=True)
    w = df.coalesce(1).write.format(fmt)
    for k, v in opts.items():
        w = w.option(k, v)
    w.save(str(path))
    return next(p for p in path.iterdir() if p.suffix in (".orc", ".parquet"))


def varint(b, i):
    v, shift = 0, 0
    while True:
        x = b[i]; i += 1
        v |= (x & 0x7F) << shift; shift += 7
        if x < 0x80:
            return v, i


def postscript(b):
    """Decode the ORC PostScript message (protobuf) into {field number: value}."""
    out, i = {}, 0
    while i < len(b):
        key, i = varint(b, i)
        field, wire = key >> 3, key & 7
        if wire == 0:
            out[field], i = varint(b, i)
        elif wire == 2:
            n, i = varint(b, i)
            raw = b[i:i + n]; i += n
            out[field] = raw.decode() if field == 8000 else list(raw)
    return out


def hexline(data, off):
    chunk = data[off:off + 16]
    return f"  {off:08x}  {' '.join(f'{b:02x}' for b in chunk):<47}  {''.join(chr(b) if 32 <= b < 127 else '.' for b in chunk)}"


if "dump" in WANT:
    print("=== 1. the bytes at the two ends of an ORC file (the 265 zones)")
    zones = spark.read.option("header", True).option("inferSchema", True).csv(str(ZONES))
    f = one_file(zones, "zones")
    data = f.read_bytes()
    print(f"  {f.name[:24]}...: {len(data):,} bytes")
    print("  the first 16 bytes:"); print(hexline(data, 0))
    print("  the last 32 bytes:"); print(hexline(data, len(data) - 32)); print(hexline(data, len(data) - 16))
    ps_len = data[-1]
    print(f"  last byte: postscript length {ps_len}; postscript is bytes {len(data) - 1 - ps_len:,} to {len(data) - 2:,}")
    ps = postscript(data[len(data) - 1 - ps_len:len(data) - 1])
    print(f"  the postscript, decoded by hand (protobuf field: value): {ps}")
    meta = json.loads(tools("meta", "-j", str(f)))
    si = meta["stripes"][0]["stripeInformation"]
    stripe_end = si["offset"] + si["indexLength"] + si["dataLength"] + si["footerLength"]
    print(f"  the layout: 'ORC' 0-2 | stripe {si['offset']}-{stripe_end - 1} | metadata {ps[5]} B | footer {ps[1]} B | "
          f"postscript {ps_len} B | 1 length byte -> {3 + stripe_end - si['offset'] + ps[5] + ps[1] + ps_len + 1:,} bytes")
    print(f"  orc-tools meta -j: fileVersion {meta['fileVersion']}, writerVersion {meta['writerVersion']}, "
          f"softwareVersion {meta['softwareVersion']}, compression {meta['compression']}, calendar {meta['calendar']}")
    print(f"    schema {meta['schemaString']}")
    print(f"    userMetadata {meta['userMetadata']}")
    for s in meta["stripes"]:
        print(f"    stripe {s['stripeNumber']}: offset {s['stripeInformation']['offset']}, index {s['stripeInformation']['indexLength']}, "
              f"data {s['stripeInformation']['dataLength']}, footer {s['stripeInformation']['footerLength']}, rows {s['stripeInformation']['rowCount']}")
        for st in s["streams"]:
            print(f"      stream column {st['columnId']} {st['section']:16} start {st['startOffset']:5} length {st['length']}")
        print("      encodings: " + ", ".join(f"col {i} {e['kind']}" + (f"[{e['dictionarySize']}]" if 'dictionarySize' in e else "")
                                       for i, e in enumerate(s["encodings"])))
    for c in meta["fileStatistics"]:
        extra = f" min {c.get('min')!r} max {c.get('max')!r}" if "min" in c else ""
        print(f"    file statistics column {c['columnId']}: count {c['count']} hasNull {c['hasNull']}{extra}")

    print("\n=== 2. the June trips as ORC: stripes and the row index")
    trips = spark.read.parquet(str(TRIPS))
    f = one_file(trips.orderBy("PULocationID"), "trips_one")
    meta = json.loads(tools("meta", "-j", str(f)))
    print(f"  {f.stat().st_size / 2**20:.1f} MB, rows {meta['numberOfRows']:,}, compression {meta['compression']}, "
          f"{len(meta['stripes'])} stripes")
    for s in meta["stripes"]:
        si = s["stripeInformation"]
        print(f"    stripe {s['stripeNumber']:2}: rows {si['rowCount']:>9,}  index {si['indexLength']:>7,} B  data {si['dataLength']:>11,} B  footer {si['footerLength']:>5,} B")
    col = [i for i, n in enumerate(meta["schemaString"][7:-1].split(",")) if n.startswith("PULocationID")][0] + 1
    txt = tools("meta", f"--rowindex={col}", str(f))
    ri = [l.strip() for l in txt.splitlines() if l.strip().startswith("Entry")][:4]
    print(f"  row index of PULocationID (column {col}), stripe 1, first entries:")
    for l in ri:
        print("    " + re.sub(r"\s+", " ", l)[:110])

if "readers" in WANT:
    print("\n=== 3. two ORC readers: spark.sql.orc.impl")
    path = OUT / "trips_one"
    for impl in ("native", "hive"):
        spark.conf.set("spark.sql.orc.impl", impl)
        for vec in ("true", "false"):
            spark.conf.set("spark.sql.orc.enableVectorizedReader", vec)
            df = spark.read.orc(str(path)).where("PULocationID = 132").select("tip_amount")
            jfmt = df._jdf.queryExecution().executedPlan().collectLeaves().head().relation().fileFormat().getClass().getName()
            scan = [l.strip() for l in plan(df, "simple").splitlines() if "FileScan" in l][0]
            batched = re.search(r"Batched: (\w+)", scan).group(1)
            pushed = re.search(r"PushedFilters: \[([^\]]*)\]", scan).group(1)
            print(f"  impl={impl:6} vectorized={vec:5} -> {jfmt}  Batched: {batched}  PushedFilters: [{pushed}]")
    spark.conf.unset("spark.sql.orc.impl"); spark.conf.unset("spark.sql.orc.enableVectorizedReader")


@contextlib.contextmanager
def bench_lock():
    if os.environ.get("BOOK_NO_LOCK"):
        yield
        return
    with open(DATA / ".bench.lock", "a") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)


def rows_scanned(df, label):
    spark.sparkContext.setJobDescription(label)
    df.collect()
    spark.sparkContext.setJobDescription(None)
    q = [x for x in ui_api("sql?details=true&planDescription=false&length=100000") if x["description"] == label][-1]
    for n in q["nodes"]:
        if n["nodeName"].startswith("Scan"):
            m = {x["name"]: x["value"] for x in n["metrics"]}
            return int(m["number of output rows"].replace(",", ""))


if "compare" in WANT:
    print("\n=== 4. Parquet against ORC on the June trips (same rows, sorted by PULocationID, 4 files each)")
    trips = spark.read.parquet(str(TRIPS)).repartitionByRange(4, "PULocationID").sortWithinPartitions("PULocationID")
    paths = {}
    for fmt in ("parquet", "orc"):
        p = OUT / f"cmp_{fmt}"
        shutil.rmtree(p, ignore_errors=True)
        trips.write.format(fmt).save(str(p))
        files = [x for x in p.iterdir() if x.suffix == f".{fmt}" or x.name.endswith(f".{fmt}")]
        paths[fmt] = p
        print(f"  {fmt:8} {len(files)} files, {sum(x.stat().st_size for x in files) / 2**20:.1f} MB "
              f"(codec {spark.conf.get(f'spark.sql.{fmt}.compression.codec')})")
    queries = {"PULocationID = 132": lambda d: d.where("PULocationID = 132").agg(F.sum("tip_amount")),
               "tip_amount > 50": lambda d: d.where("tip_amount > 50").agg(F.count("*")),
               "full scan, 4 columns": lambda d: d.agg(F.sum("tip_amount"), F.avg("trip_distance"),
                                                       F.max("fare_amount"), F.countDistinct("DOLocationID"))}
    setups = [("parquet", "vectorized", {"spark.sql.parquet.enableVectorizedReader": "true"}),
              ("parquet", "row by row", {"spark.sql.parquet.enableVectorizedReader": "false"}),
              ("parquet", "no pushdown", {"spark.sql.parquet.filterPushdown": "false"}),
              ("orc", "native, vectorized", {"spark.sql.orc.impl": "native"}),
              ("orc", "native, row by row", {"spark.sql.orc.impl": "native", "spark.sql.orc.enableVectorizedReader": "false"}),
              ("orc", "native, no pushdown", {"spark.sql.orc.impl": "native", "spark.sql.orc.filterPushdown": "false"}),
              ("orc", "hive reader", {"spark.sql.orc.impl": "hive"})]
    print(f"\n  {'format, reader':30}" + "".join(f"{q:>26}" for q in queries))
    print(f"  {'':30}" + "".join(f"{'rows from scan   median':>26}" for q in queries))
    for fmt, label, conf in setups:
        for k, v in conf.items():
            spark.conf.set(k, v)
        cells = []
        for qn, qf in queries.items():
            rows = rows_scanned(qf(spark.read.format(fmt).load(str(paths[fmt]))), f"{fmt} {label} {qn}")
            with bench_lock():
                qf(spark.read.format(fmt).load(str(paths[fmt]))).collect()
                ts = []
                for _ in range(5):
                    t0 = time.perf_counter()
                    qf(spark.read.format(fmt).load(str(paths[fmt]))).collect()
                    ts.append(time.perf_counter() - t0)
            cells.append(f"{rows:>14,} {statistics.median(ts):>7.2f} s")
        print(f"  {fmt + ', ' + label:30}" + "".join(f"{c:>26}" for c in cells))
        for k in conf:
            spark.conf.unset(k)

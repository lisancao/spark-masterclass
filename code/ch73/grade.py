"""Chapter 73: the grader for casefile. Plain Python stages first, then Spark stages.

Run from code/:        uv run python ch73/grade.py
One or more stages:    ONLY=parsing,planning uv run python ch73/grade.py
"""
import datetime
import json
import os
import re
import shutil
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from pyspark.sql import SparkSession, functions as F  # noqa: E402
import casefile  # noqa: E402
from casefile import *  # noqa: E402,F401,F403
from casefile import (CaseFileReader, CaseFileSource, CaseFiles, attachment_status, change_rows,  # noqa: E402
                      columns_of, list_case_folders, parse_change_line, parse_index, plan_by_bytes,
                      read_manifest)

SAMPLE = str(Path(__file__).resolve().parents[1] / "data" / "messy" / "casefiles")


class OfflineTests(unittest.TestCase):
    """Run without a SparkSession. The harness also runs them against
    broken versions of your functions; each broken version must fail at
    least one test."""

    def test_v2_index(self):
        with open(os.path.join(SAMPLE, "CASE-0001", "case.json"), "rb") as f:
            i = parse_index(f.read(), "CASE-0001")
        self.assertEqual((i["case_id"], i["schema_version"], i["status"], i["priority"]), ("CASE-0001", 2, "open", 2))
        self.assertEqual(i["opened"], datetime.datetime(2026, 3, 2, 9, 15, tzinfo=datetime.timezone.utc))
        self.assertEqual(i["problems"], [])

    def test_v1_index(self):
        raw = b'{"id": "case-7", "created": 0, "state": "OPEN", "prio": 2, "labels": "a, b", "attachments": ["x.png"]}'
        i = parse_index(raw, "CASE-7")
        self.assertEqual((i["case_id"], i["schema_version"], i["status"], i["priority"], i["tags"]),
                         ("CASE-7", 1, "open", 2, ["a", "b"]))
        self.assertEqual(i["listed"], [{"name": "x.png", "sha256": None, "bytes": None}])

    def test_bom_and_offset(self):
        raw = "\ufeff".encode() + b'{"schema": 2, "case_id": "C-9", "opened": "2026-03-21T10:30:00+02:00"}'
        i = parse_index(raw, "c-9")
        self.assertEqual(i["problems"], [])
        self.assertEqual(i["opened"], datetime.datetime(2026, 3, 21, 8, 30, tzinfo=datetime.timezone.utc))

    def test_bad_index_and_priority(self):
        self.assertEqual(parse_index(b'{"schema": 2, "case', "CASE-8")["problems"], ["bad_index"])
        self.assertEqual(parse_index(b'{"schema": 2, "case_id": "X", "priority": "urgent"}', "X")["problems"],
                         ["bad_priority"])

    def test_change_lines(self):
        c = parse_change_line("2026-03-21T09:00:00Z | status | new -> open | dev\r")
        self.assertEqual((c["field"], c["old"], c["new"], c["actor"]), ("status", "new", "open", "dev"))
        self.assertIsNone(parse_change_line("2026-03-25T09:30:00Z | status | open triage | dev"))
        self.assertIsNone(parse_change_line("status changed by ana"))
        rows = list(change_rows("C", "# header\n\n2026-03-21T09:00:00Z | status | a -> b | x\nnonsense", 1))
        self.assertEqual([(r[1], r[8]) for r in rows], [(3, None), (4, "unparsed")])

    def test_attachment_status(self):
        self.assertEqual(attachment_status("a", "a", True, True), "ok")
        self.assertEqual(attachment_status("a", "b", True, True), "sha_mismatch")
        self.assertEqual(attachment_status(None, "b", True, True), "unverified")
        self.assertEqual(attachment_status("a", None, True, False), "missing")
        self.assertEqual(attachment_status(None, "b", False, True), "unlisted")

    def test_plan_by_bytes(self):
        groups = plan_by_bytes([("a", 50), ("b", 30), ("c", 30), ("d", 20), ("big", 500)], 60)
        self.assertEqual(groups, [["big"], ["a"], ["b", "c"], ["d"]])
        sizes = {"a": 50, "b": 30, "c": 30, "d": 20, "big": 500}
        for g in groups:
            self.assertTrue(len(g) == 1 or sum(sizes[k] for k in g) <= 60)
        self.assertEqual(plan_by_bytes([], 10), [])

    def test_reader_without_spark(self):
        from pyspark import cloudpickle

        reader = CaseFileReader({"path": SAMPLE, "table": "attachments", "maxPartitionBytes": "16384"}, None)
        parts = reader.partitions()
        cloudpickle.loads(cloudpickle.dumps((reader, parts)))
        rows = [r for p in parts for r in reader.read(p)]
        status = {(r[0], r[2]): r[7] for r in rows}
        self.assertEqual(status[("CASE-0004", "scan.png")], "missing")
        self.assertEqual(status[("CASE-0006", "receipt.pdf")], "sha_mismatch")
        self.assertEqual(status[("CASE-0005", "stray.tmp")], "unlisted")


# =====================================================================
# Stage "design_review"
# =====================================================================
DESIGN_REVIEW = """
## Decisions

- One source, three tables chosen with the `table` option, because the
  three grains (case, attachment, change line) cannot share one schema
  without a lot of NULLs, and a consumer usually needs one of them.
- Partitions are planned by bytes with first fit decreasing over files
  (attachments) or index and log sizes (cases, changes), so the large
  recording does not share a task with everything else.
- Loose case metadata is VARIANT: its keys differ per case and per
  schema version, and VARIANT keeps it queryable without a schema.
- Stream offsets are byte positions per change log, plus a line count
  for line numbers; only complete lines (ending in a newline) are read.
- A write publishes by replacing one manifest file; data folders are
  named per job and never renamed over live data.

## Trade-offs

- Pushdown on status and opened reads every index on the driver during
  planning, then again on the executor. For thousands of cases a small
  catalog file would be cheaper than opening each case.json twice.
- Pushdown is opt-in. It saves I/O but needs the internal flag, and on
  Spark 4.1 to 4.3 a reused DataFrame can leak pushed filters, so the
  default reader has no pushFilters and CaseFiles loads afresh per table.
- The manifest makes writes atomic for readers of this connector only;
  a tool that lists folders directly sees uncommitted job folders.

## Failure modes

- A task fails during write: abort() deletes the folders of this job;
  the manifest is untouched, so readers never see a partial write.
- Two rows with one case_id, or an append of an existing case: commit()
  raises before the manifest changes, and Spark then calls abort().
- The driver dies between os.replace of the manifest and the cleanup of
  replaced folders: the data is correct; old folders are left behind as
  garbage that a later overwrite or a vacuum must remove.
- A change log is appended while a batch runs: offsets only cover
  complete lines that existed when latestOffset ran, so a half-written
  line waits for the next batch.
- A change log is rewritten instead of appended: byte offsets become
  wrong. The format must be append-only; detecting a shrink (size below
  the offset) and failing would be the next improvement.
- A corrupt or truncated index: the case still appears, with NULL
  fields and the problem bad_index, instead of failing the query.

## Testing

- Plain unittest tests for every parser and the planner, run without
  Spark, and checked by the harness against broken versions.
- Integration tests in a local session: plans, audit files to prove
  skipped folders, the writer under injected failures, and the stream
  compared with the batch reader of the same folder.

## What I would change for production

- A catalog file of index summaries so planning does not open every
  case.json; a vacuum command for folders no manifest points to.
- Out-of-order change lines are kept as they are; ordering by ts is the
  consumer's job, and flagging them in a stream needs state per log.
- Package it as pyspark_casefile with DefaultSource and the unit tests.
"""


# =====================================================================
# The harness (given): every stage is graded on its own
# =====================================================================
DATA = SAMPLE
EXPECTED_PROBLEMS = {
    "CASE-0004": ["missing_attachment"], "CASE-0005": ["unlisted_attachment"],
    "CASE-0006": [], "CASE-0008": ["bad_index"], "CASE-0010": ["bad_priority"],
}


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def stage_parsing(ctx):
    with open(os.path.join(DATA, "CASE-0007", "case.json"), "rb") as f:
        v1 = parse_index(f.read(), "CASE-0007")
    check(v1["schema_version"] == 1 and v1["case_id"] == "CASE-0007", f"CASE-0007 is a version 1 index, got {v1}")
    check(v1["opened"] == datetime.datetime(2026, 3, 4, 9, 30, tzinfo=datetime.timezone.utc),
          f"version 1 'created' is epoch seconds (1772616600 = 2026-03-04 09:30 UTC), got {v1['opened']}")
    check((v1["status"], v1["priority"], v1["tags"]) == ("open", 2, ["billing", "vip"]),
          f"v1 state/prio/labels should become ('open', 2, ['billing', 'vip']), got "
          f"{(v1['status'], v1['priority'], v1['tags'])}")
    check(v1["meta"] == {"imported_from": "legacy-crm", "legacy_id": 7731}, "v1 'extra' is the metadata")
    with open(os.path.join(DATA, "case-0009", "case.json"), "rb") as f:
        v9 = parse_index(f.read(), "case-0009")
    check(v9["problems"] == [] and v9["case_id"] == "CASE-0009",
          f"case-0009 starts with a byte order mark; decode with utf-8-sig. Got problems {v9['problems']}")
    check(v9["opened"] == datetime.datetime(2026, 3, 21, 8, 30, tzinfo=datetime.timezone.utc),
          f"2026-03-21T10:30:00+02:00 is 08:30 UTC, got {v9['opened']}")
    with open(os.path.join(DATA, "CASE-0008", "case.json"), "rb") as f:
        v8 = parse_index(f.read(), "CASE-0008")
    check(v8["problems"] == ["bad_index"] and v8["case_id"] == "CASE-0008",
          f"a truncated index gives case_id from the folder and problems ['bad_index'], got {v8}")
    with open(os.path.join(DATA, "CASE-0010", "case.json"), "rb") as f:
        v10 = parse_index(f.read(), "CASE-0010")
    check(v10["priority"] is None and v10["problems"] == ["bad_priority"],
          f"priority 'urgent' is not P1 to P4: priority None, problems ['bad_priority']; got {v10}")
    c = parse_change_line("2026-03-21T09:00:00Z | status | new -> open | dev\r")
    check(c is not None and (c["old"], c["new"], c["actor"]) == ("new", "open", "dev"),
          f"a CRLF change line should parse without the \\r, got {c}")
    check(parse_change_line("2026-03-25T09:30:00Z | status | open triage | dev") is None,
          "a line without '->' is not a change")
    check([attachment_status("a", "a", True, True), attachment_status("a", "b", True, True),
           attachment_status(None, "b", True, True), attachment_status("a", None, True, False),
           attachment_status(None, "b", False, True)] == ["ok", "sha_mismatch", "unverified", "missing", "unlisted"],
          "attachment_status should give ok, sha_mismatch, unverified, missing, unlisted for the five cases")
    return "v1 and v2 indexes, BOM, offsets, bad index, priorities, change lines, statuses"


def stage_planning(ctx):
    items = [("a", 50), ("b", 30), ("c", 30), ("d", 20), ("big", 500)]
    got = plan_by_bytes(items, 60)
    check(got == [["big"], ["a"], ["b", "c"], ["d"]],
          f"plan_by_bytes({items}, 60) should be [['big'], ['a'], ['b', 'c'], ['d']] (first fit decreasing), got {got}")
    check(plan_by_bytes([], 10) == [], "no items, no groups")
    folders = list_case_folders(DATA)
    check(folders == ["CASE-0001", "CASE-0002", "CASE-0003", "CASE-0004", "CASE-0005", "CASE-0006",
                      "CASE-0007", "CASE-0008", "CASE-0010", "case-0009"],
          f"list_case_folders should find the 10 case folders and skip README.txt, .DS_Store and _staging; got {folders}")
    reader = CaseFileReader({"path": DATA, "table": "attachments"}, None)
    parts = reader.partitions()
    sizes = {(f, n): os.path.getsize(os.path.join(DATA, f, "attachments", n))
             for p in parts for f, n in p.items if n is not None}
    for p in parts:
        total = sum(sizes.get(i, 0) for i in p.items)
        check(len(p.items) == 1 or total <= 16384,
              f"a partition holds {total} bytes (> maxPartitionBytes 16384) in {len(p.items)} files")
    big = [p for p in parts if ("CASE-0002", "call.wav") in p.items]
    check(len(big) == 1 and len(big[0].items) == 1, "the 40 KB call.wav should have a partition of its own")
    check(len(parts) == 2, f"with the default 16384 bytes the attachments plan into 2 partitions, got {len(parts)}")
    return f"{len(parts)} attachment partitions, call.wav alone"


def stage_batch(ctx):
    spark = ctx["spark"]
    cases = spark.read.format("casefile").option("table", "cases").load(DATA)
    check(cases.columns == columns_of("cases"), f"cases columns should follow SCHEMAS['cases'], got {cases.columns}")
    rows = {r["case_id"]: r for r in cases.collect()}
    check(len(rows) == 10, f"10 cases expected, got {sorted(rows)}")
    for case_id, problems in EXPECTED_PROBLEMS.items():
        check(rows[case_id]["problems"] == problems, f"{case_id} problems should be {problems}, got {rows[case_id]['problems']}")
    check(rows["CASE-0002"]["bytes"] == 40124, f"CASE-0002 holds 40124 bytes of attachments, got {rows['CASE-0002']['bytes']}")
    check(rows["CASE-0004"]["attachments"] == 2, "CASE-0004 lists 2 attachments (one is missing)")
    att = spark.read.format("casefile").option("table", "attachments").load(DATA)
    by_status = {r["status"]: r["n"] for r in att.groupBy("status").agg(F.count("*").alias("n")).collect()}
    check(by_status == {"ok": 10, "missing": 1, "unlisted": 2, "sha_mismatch": 1, "unverified": 1},
          f"attachment statuses should be ok 10, missing 1, unlisted 2 (stray.tmp and CASE-0008's note), "
          f"sha_mismatch 1, unverified 1 (the v1 case); got {by_status}")
    check(att.select(F.spark_partition_id()).distinct().count() == 2, "attachments should read in 2 partitions")
    ch = spark.read.format("casefile").option("table", "changes").load(DATA)
    n = ch.count()
    bad = sorted((r["case_id"], r["line"]) for r in ch.filter("problem = 'unparsed'").collect())
    check(n == 44, f"the change logs hold 44 change lines (comments and blank lines are not rows), got {n}")
    check(bad == [("CASE-0004", 6), ("CASE-0010", 4)], f"unparsed lines should be CASE-0004 line 6 and CASE-0010 line 4, got {bad}")
    first9 = ch.filter("case_id = 'CASE-0009'").orderBy("line").first()
    check(first9["field"] == "status" and first9["line"] == 1, "case-0009's first line (after the BOM) is a status change")
    return "10 cases, 15 attachment rows, 44 changes"


def stage_variant(ctx):
    spark = ctx["spark"]
    cases = spark.read.format("casefile").option("table", "cases").load(DATA)
    check(dict(cases.dtypes)["meta"] == "variant", f"meta should be VARIANT, got {dict(cases.dtypes)['meta']}")
    got = {r["case_id"]: (r["tier"], r["sla"]) for r in cases.select(
        "case_id", F.expr("variant_get(meta, '$.customer.tier', 'string')").alias("tier"),
        F.expr("try_variant_get(meta, '$.sla_hours', 'int')").alias("sla")).collect()}
    check(got["CASE-0001"] == ("gold", 24) and got["CASE-0010"] == ("gold", 4) and got["CASE-0002"] == (None, None),
          f"variant_get on meta: CASE-0001 ('gold', 24), CASE-0010 ('gold', 4), CASE-0002 (None, None); got "
          f"{got.get('CASE-0001'), got.get('CASE-0010'), got.get('CASE-0002')}")
    name = cases.filter("case_id = 'CASE-0009'").select(
        F.expr("variant_get(meta, '$.customer.name', 'string')")).first()[0]
    check(name == "Hélène Côté", f"non-ASCII metadata should survive: got {name!r}")
    legacy = cases.filter("case_id = 'CASE-0007'").select(F.expr("variant_get(meta, '$.legacy_id', 'int')")).first()[0]
    check(legacy == 7731, "the v1 case's 'extra' object is its meta")
    return "meta is VARIANT and queryable"


def plan_text(df):
    import contextlib
    import io

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain("formatted")
    return buf.getvalue()


def audited(spark, table, condition, **options):
    audit = tempfile.mkdtemp(prefix="audit_")
    df = CaseFiles(spark, DATA, pushdown="true", audit=audit, **options).table(table).filter(condition)
    rows = df.collect()
    opened = sorted({name.split("-", 2)[2].rsplit("-", 1)[0] for name in os.listdir(audit)})
    return df, rows, opened


def stage_pushdown(ctx):
    spark = ctx["spark"]
    flag = "spark.sql.python.filterPushdown.enabled"
    spark.conf.set(flag, "false")
    try:
        n = spark.read.format("casefile").option("table", "cases").load(DATA).filter("status = 'open'").count()
    except Exception as e:  # noqa: BLE001
        raise AssertionError("with the flag off and no pushdown option the default reader must work; it failed: "
                             + explain_error(e)) from None
    finally:
        spark.conf.set(flag, "true")
    check(n == 5, f"5 cases are open, got {n}")

    df, rows, opened = audited(spark, "cases", "status = 'open'")
    check(len(rows) == 5, f"status = 'open' should give 5 cases with pushdown, got {len(rows)}")
    plan = plan_text(df)
    check("PushedFilters: [EqualTo(status,open)" in plan, "the plan should list EqualTo(status,open) under PushedFilters")
    check(opened == ["CASE-0001", "CASE-0005", "CASE-0007", "CASE-0010", "case-0009"],
          f"tasks should open only the folders of the 5 open cases, got {opened}")

    window = "opened >= timestamp'2026-03-10 00:00:00' AND opened < timestamp'2026-03-22 00:00:00'"
    df, rows, opened = audited(spark, "cases", window)
    check(sorted(r["case_id"] for r in rows) == ["CASE-0005", "CASE-0006", "CASE-0009"],
          f"cases opened from 2026-03-10 to before 2026-03-22 (UTC): CASE-0005, CASE-0006, CASE-0009; got "
          f"{sorted(r['case_id'] for r in rows)}")
    check(opened == ["CASE-0005", "CASE-0006", "case-0009"], f"only those folders should be opened, got {opened}")

    df, rows, opened = audited(spark, "attachments", "case_id IN ('CASE-0002', 'CASE-0004')")
    check(len(rows) == 4 and opened == ["CASE-0002", "CASE-0004"],
          f"attachments of CASE-0002 and CASE-0004: 4 rows from 2 folders; got {len(rows)} rows from {opened}")
    _, rows, opened = audited(spark, "changes", "case_id = 'CASE-0003' AND field = 'status'")
    check(len(rows) == 3 and opened == ["CASE-0003"], f"CASE-0003 has 3 status changes; got {len(rows)} from {opened}")

    # The 4.2.0 leak: the default reader is safe on a reused DataFrame...
    plain = spark.read.format("casefile").option("table", "cases").load(DATA)
    seq = [plain.count(), plain.filter("status = 'open'").count(), plain.count()]
    check(seq == [10, 5, 10], f"the default reader must not leak on a reused DataFrame: {seq}")
    # ...and CaseFiles gives fresh loads with pushdown on
    cf = CaseFiles(spark, DATA, pushdown="true")
    check(cf.table("cases").filter("status = 'open'").count() == 5 and cf.table("cases").count() == 10,
          "CaseFiles.table() should load afresh each time")
    joined = cf.table("cases").alias("c").join(cf.table("attachments").alias("a"), "case_id") \
        .filter("c.status = 'open'").count()
    check(joined == 7, f"open cases joined with their attachment rows: expected 7, got {joined}")
    return "flag off works, pushed filters skip folders, no leak through CaseFiles"


def stage_writer(ctx):
    spark = ctx["spark"]
    out = os.path.join(tempfile.mkdtemp(prefix="cases_out_"), "export")
    source = spark.read.format("casefile").option("table", "cases").load(DATA)
    cols = ["case_id", "status", "priority", "opened", "tags", "meta"]

    def visible():
        return sorted(r["case_id"] for r in spark.read.format("casefile").option("table", "cases").load(out).collect())

    def try_save(df, mode, label):
        try:
            df.write.format("casefile").mode(mode).save(out)
        except Exception as e:  # noqa: BLE001
            return str(e)
        if label:
            raise AssertionError(f"{label} should have failed")
        return None

    err = try_save(source.filter("case_id <= 'CASE-0005'").select(*cols), "append", None)
    check(err is None, f"the first append failed: {err and explain_error(Exception(err))}")
    check(visible() == ["CASE-0001", "CASE-0002", "CASE-0003", "CASE-0004", "CASE-0005"], f"after append: {visible()}")
    m = read_manifest(out)
    check(m["version"] == 1 and len(m["cases"]) == 5, f"the manifest should list 5 cases at version 1, got {m}")

    before = (json.dumps(read_manifest(out), sort_keys=True), sorted(os.listdir(out)))
    # One write task, in case_id order: it writes four case folders, then write()
    # must reject the invalid case_id. (A failure injected with raise_error does
    # not test abort(): the JVM feeds rows to Python from its own thread, and in
    # testing the task always failed before write() had written anything.)
    broken = (source.filter("case_id > 'CASE-0005'").select(*cols)
              .withColumn("case_id", F.when(F.col("case_id") == "CASE-0010", F.lit("not a case"))
                          .otherwise(F.col("case_id")))
              .orderBy("case_id").coalesce(1))
    err = try_save(broken, "append", "a write with an invalid case_id")
    check("invalid case_id" in err, "write() should reject case_id 'not a case' with a message containing "
          "'invalid case_id', got: " + explain_error(Exception(err)))
    after = (json.dumps(read_manifest(out), sort_keys=True), sorted(os.listdir(out)))
    check(before == after, "a failed write must leave the manifest and the folder exactly as they were "
          f"(abort() should delete this job's folders); new entries: {sorted(set(after[1]) - set(before[1]))}")

    dup = source.filter("case_id = 'CASE-0006'").select(*cols)
    err = try_save(dup.union(dup).repartition(2), "append", "a write with a duplicate case_id")
    check("duplicate" in err, "a duplicate case_id should fail in commit() with a message about duplicates, got: "
          + explain_error(Exception(err)))
    clash = try_save(source.filter("case_id = 'CASE-0001'").select(*cols), "append", "appending an existing case")
    check("exists" in clash, "appending an existing case_id should fail in commit() with 'already exists', got: "
          + explain_error(Exception(clash)))
    check((json.dumps(read_manifest(out), sort_keys=True), sorted(os.listdir(out))) == before,
          "rejected commits must not change anything")

    err = try_save(source.filter("case_id > 'CASE-0005'").select(*cols), "overwrite", None)
    check(err is None, f"overwrite failed: {err and explain_error(Exception(err))}")
    check(visible() == ["CASE-0006", "CASE-0007", "CASE-0008", "CASE-0009", "CASE-0010"], f"after overwrite: {visible()}")
    check(len([e for e in os.listdir(out) if not e.startswith((".", "_"))]) == 5,
          "overwrite should delete the replaced folders after the manifest swap")
    back = spark.read.format("casefile").option("table", "cases").load(out)
    a = {tuple(r) for r in back.select("case_id", "status", "priority", "opened", F.to_json("meta")).collect()}
    b = {tuple(r) for r in source.filter("case_id > 'CASE-0005'")
         .select("case_id", "status", "priority", "opened", F.to_json("meta")).collect()}
    check(a == b, f"a round trip should keep case_id, status, priority, opened and meta: {sorted(a ^ b)[:3]}")
    return "append, failed task, duplicate, clash, overwrite, round trip"


def stage_streaming(ctx):
    spark = ctx["spark"]
    base = tempfile.mkdtemp(prefix="stream_")
    live, sink, ckpt = (os.path.join(base, d) for d in ("live", "sink", "ckpt"))
    shutil.copytree(DATA, live)

    def run(trigger):
        q = (spark.readStream.format("casefile").option("table", "changes")
             .option("maxBytesPerTrigger", 400).load(live)
             .writeStream.format("parquet").option("path", sink).option("checkpointLocation", ckpt)
             .trigger(**trigger).start())
        if "availableNow" in trigger:
            q.awaitTermination()
        else:
            q.processAllAvailable()
            q.stop()
        return [(p.batchId, p.numInputRows) for p in q.recentProgress if p.numInputRows]

    def offsets(batch):
        with open(os.path.join(ckpt, "offsets", str(batch))) as f:
            return json.loads(f.read().splitlines()[-1])

    def consumed(a, b):
        return sum(b["pos"][k][0] - a["pos"].get(k, [0, 0])[0] for k in b["pos"])

    first = run({"availableNow": True})
    check(len(first) >= 5, f"with maxBytesPerTrigger 400 the first run should take several batches, got {first}")
    prev = {"v": 1, "pos": {}}
    for batch, _ in first:
        cur = offsets(batch)
        check(set(cur) == {"v", "pos"}, f"offsets should look like {{'v': 1, 'pos': {{folder: [byte, lines]}}}}, got {cur}")
        used = consumed(prev, cur)
        check(used <= 400, f"batch {batch} consumed {used} bytes of change logs; maxBytesPerTrigger is 400")
        prev = cur
    check(prev["pos"]["CASE-0003"][1] == 3, "CASE-0003's last line has no newline yet: only 3 lines may be consumed")

    with open(os.path.join(live, "CASE-0003", "changes.log"), "ab") as f:
        f.write(b"\n2026-03-05T10:00:00Z | status | triage -> open | ana\n2026-03-05T10:05:00Z | priority | P1")
    os.makedirs(os.path.join(live, "CASE-0011", "attachments"))
    with open(os.path.join(live, "CASE-0011", "case.json"), "w") as f:
        json.dump({"schema": 2, "case_id": "CASE-0011", "opened": "2026-03-27T08:00:00Z", "status": "new"}, f)
    with open(os.path.join(live, "CASE-0011", "changes.log"), "w") as f:
        f.write("2026-03-27T08:00:00Z | status | new -> open | binh\n2026-03-27T08:10:00Z | status | open -> waiting | binh\n")
    second = run({"processingTime": "0 seconds"})
    check(second and second[0][0] == first[-1][0] + 1, f"the restart should continue after batch {first[-1][0]}, got {second}")
    check(sum(n for _, n in second) == 4, f"the restart should read exactly the 4 new complete lines, got {second}")

    with open(os.path.join(live, "CASE-0003", "changes.log"), "ab") as f:
        f.write(b" -> P2 | ana\n")
    third = run({"availableNow": True})
    check(sum(n for _, n in third) == 1, f"completing the partial line should give exactly 1 row, got {third}")

    streamed = spark.read.parquet(sink)
    batch = spark.read.format("casefile").option("table", "changes").load(live)
    key = ["case_id", "line"]
    check(streamed.count() == streamed.select(*key).distinct().count(), "the stream wrote a line twice")
    diff = streamed.select(*batch.columns).exceptAll(batch).count() + batch.exceptAll(streamed.select(*batch.columns)).count()
    check(diff == 0, f"the stream's rows should equal the batch reader's rows for the same folder ({diff} differ)")
    return f"{len(first)} + {len(second)} + {len(third)} batches, stream == batch, {streamed.count()} rows"


def stage_logs(ctx):
    spark = ctx["spark"]
    spark.conf.set("spark.sql.pyspark.worker.logging.enabled", "true")
    try:
        spark.read.format("casefile").option("table", "cases").load(DATA).collect()
        spark.read.format("casefile").option("table", "attachments").load(DATA).collect()
        expected = {"CASE-0004 missing_attachment", "CASE-0005 unlisted_attachment", "CASE-0008 bad_index",
                    "CASE-0010 bad_priority", "CASE-0004 missing scan.png", "CASE-0006 sha_mismatch receipt.pdf"}
        seen = set()
        for _ in range(30):    # worker logs arrive asynchronously (see "Packaging, Registration, ...")
            seen = {r["msg"] for r in spark.tvf.python_worker_logs().filter("logger = 'casefile'").collect()}
            if expected <= seen:
                break
            time.sleep(0.5)
    finally:
        spark.conf.set("spark.sql.pyspark.worker.logging.enabled", "false")
    check(expected <= seen, f"logger 'casefile' should warn about every problem; missing: {sorted(expected - seen)}")
    return f"{len(seen)} distinct warnings from executors"


MUTANTS = {
    "a planner that ignores the byte target": ("plan_by_bytes", lambda real: (
        lambda items, target: [[k for k, _ in sorted(items, key=lambda kv: (-kv[1], kv[0]))]] if items else [])),
    "a planner that splits by count, not bytes": ("plan_by_bytes", lambda real: (
        lambda items, target: [[k for k, _ in sorted(items)][i::2] for i in range(2) if sorted(items)[i::2]])),
    "an index parser that forgets the BOM": ("parse_index", lambda real: (
        lambda raw, folder: real(raw, folder) if not raw.startswith(b"\xef\xbb\xbf")
        else {**real(b"{", folder)})),
    "an index parser that treats version 1 as broken": ("parse_index", lambda real: (
        lambda raw, folder: (lambda r: {**real(b"{", folder)} if r["schema_version"] == 1 else r)(real(raw, folder)))),
    "an index parser that ignores time zone offsets": ("parse_index", lambda real: (
        lambda raw, folder: (lambda r: {**r, "opened": r["opened"].replace(hour=(r["opened"].hour + 2) % 24)}
                             if r["opened"] is not None and b"+02:00" in raw else r)(real(raw, folder)))),
    "an index parser that accepts any priority": ("parse_index", lambda real: (
        lambda raw, folder: (lambda r: {**r, "problems": [p for p in r["problems"] if p != "bad_priority"]})(
            real(raw, folder)))),
    "a change parser that keeps the \\r of CRLF lines": ("parse_change_line", lambda real: (
        lambda line: (lambda r: {**r, "actor": r["actor"] + "\r"} if r and line.endswith("\r") else r)(real(line)))),
    "a change parser that accepts lines without an arrow": ("parse_change_line", lambda real: (
        lambda line: real(line) or {"ts": None, "field": "?", "old": None, "new": None, "actor": None})),
    "a checksum check that always says ok": ("attachment_status", lambda real: (
        lambda expected, actual, listed, on_disk: "ok" if listed and on_disk else real(expected, actual, listed, on_disk))),
}


def run_offline_suite():
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(OfflineTests)
    result = unittest.TestResult()
    suite.run(result)
    return result


def stage_offline_tests(ctx):
    from pyspark import SparkContext

    check(SparkContext._active_spark_context is None, "offline tests must run before any SparkSession exists")
    result = run_offline_suite()
    check(result.testsRun >= 5, f"write at least 5 tests in OfflineTests, found {result.testsRun}")
    problems = [f"{t.id()}: {tb.strip().splitlines()[-1]}" for t, tb in result.failures + result.errors]
    check(not problems, "your offline tests should pass on your code:\n  " + "\n  ".join(problems))
    check(SparkContext._active_spark_context is None, "offline tests must not start Spark")
    survivors = []
    g = globals()
    for label, (name, make) in MUTANTS.items():
        real = g[name]
        g[name] = make(real)                       # the tests call functions by name
        setattr(casefile, name, g[name])           # and so does the connector
        try:
            if run_offline_suite().wasSuccessful():
                survivors.append(label)
        finally:
            g[name] = real
            setattr(casefile, name, real)
    check(not survivors, "your tests did not catch: " + "; ".join(survivors))
    return f"{result.testsRun} tests, {len(MUTANTS)} broken versions caught"


def stage_design_review(ctx):
    text = DESIGN_REVIEW
    check("TODO" not in text, "DESIGN_REVIEW still contains TODO")
    sections = {}
    for part in re.split(r"^## ", text, flags=re.M)[1:]:
        title, _, body = part.partition("\n")
        sections[title.strip().lower()] = body
    required = ["decisions", "trade-offs", "failure modes", "testing", "what i would change for production"]
    missing = [s for s in required if s not in sections]
    check(not missing, f"DESIGN_REVIEW needs the sections {required}; missing {missing}")
    for s in required:
        words = len(sections[s].split())
        check(words >= 30, f"section '{s}' has {words} words; explain your reasoning (30 or more)")
    bullets = [l for l in sections["failure modes"].splitlines() if l.strip().startswith("- ")]
    check(len(bullets) >= 4, f"list at least 4 failure modes as '- ' bullets, found {len(bullets)}")
    decisions = sections["decisions"].lower()
    for topic in ("partition", "offset", "manifest", "variant"):
        check(topic in decisions, f"the Decisions section should explain your choice about '{topic}'")
    return "all sections present"


def explain_error(e):
    """The most useful line of an exception, even when it comes from a Python worker."""
    text = str(e)
    todo = re.search(r"TODO: [^\n]*", text) or re.search(r"TODO\b[^\n]*", text)
    if todo:
        return todo.group(0)[:400]
    errors = [l.strip() for l in text.splitlines() if re.match(r"\s*[\w.]*(Error|Exception): ", l)]
    if errors:
        return errors[-1][:400]
    first = next((l for l in text.splitlines() if l.strip()), "")
    return f"{type(e).__name__}: {first[:400]}"


STAGES = [("parsing", stage_parsing), ("planning", stage_planning), ("offline_tests", stage_offline_tests),
          ("design_review", stage_design_review), ("batch", stage_batch), ("variant", stage_variant),
          ("pushdown", stage_pushdown), ("writer", stage_writer), ("streaming", stage_streaming),
          ("logs", stage_logs)]
SPARK_STAGES = {"batch", "variant", "pushdown", "writer", "streaming", "logs"}

if __name__ == "__main__":
    ONLY = os.environ.get("ONLY", "").split(",") if os.environ.get("ONLY") else None
    selected = [(n, f) for n, f in STAGES if ONLY is None or n in ONLY]
    results, ctx = [], {}
    try:
        for name, fn in selected:
            if name in SPARK_STAGES and "spark" not in ctx:
                ctx["spark"] = (SparkSession.builder.master("local[2]").appName("ch73-grade")
                                .config("spark.ui.port", os.environ.get("SPARK_UI_PORT", "4040"))
                                .config("spark.sql.session.timeZone", "UTC")
                                .config("spark.sql.shuffle.partitions", "4")
                                .config("spark.sql.python.filterPushdown.enabled", "true")
                                .getOrCreate())
                ctx["spark"].sparkContext.setLogLevel("ERROR")
                ctx["spark"].dataSource.register(CaseFileSource)
            start = time.monotonic()
            try:
                detail = fn(ctx)
                results.append((name, True, detail))
            except AssertionError as e:
                results.append((name, False, str(e)))
            except Exception as e:  # noqa: BLE001
                results.append((name, False, explain_error(e)))
            print(f"{'PASS' if results[-1][1] else 'FAIL'}  {name:14} {time.monotonic() - start:5.1f}s  {results[-1][2]}",
                  flush=True)
    finally:
        if "spark" in ctx:
            ctx["spark"].stop()

    failed = [name for name, ok, _ in results if not ok]
    print(f"\n{len(results) - len(failed)} of {len(results)} stages passed")
    if failed:
        sys.exit(f"Stages still failing: {', '.join(failed)}")
    if ONLY is not None:
        sys.exit("The selected stages pass. Set ONLY = None to grade every stage.")
    print("All tests passed!")

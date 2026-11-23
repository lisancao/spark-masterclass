"""
Chapter 87: casefile, a complete Python data source for a messy format.

`format("casefile")` reads and writes the "case file" format in
data/messy/casefiles: a folder per case with an index (case.json), an
attachments/ folder and a change log (changes.log).

    spark.read.format("casefile").option("table", "cases").load(path)

Tables (option `table`):
    cases        one row per case; loose metadata as VARIANT
    attachments  one row per attachment (listed, on disk, or both), with
                 a checksum check; planned by bytes
    changes      one row per change-log line; also a stream with offsets
                 per file, admission control by bytes and AvailableNow

Other options: `maxPartitionBytes` (default 16384), `pushdown` ("true"
to push filters; needs spark.sql.python.filterPushdown.enabled), `audit`
(a folder: one marker file per case folder a task opens),
`maxBytesPerTrigger` (streaming, default 1024).

Writing: df.write.format("casefile").mode("append" | "overwrite").save(path)
writes one case folder per row, invisible until commit() swaps the
manifest _manifest.json in one os.replace.

The grader (grade.py, next to this file) checks each capability as a
separate stage. Register the source with spark.dataSource.register(CaseFileSource).
"""

import datetime
import json
import logging
import os
import re
import shutil
import sys
import tempfile
import time
import uuid

from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    DataSourceStreamReader,
    DataSourceWriter,
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    InputPartition,
    LessThan,
    LessThanOrEqual,
    WriterCommitMessage,
)
from pyspark.sql.streaming.datasource import ReadAllAvailable, ReadMaxBytes, SupportsTriggerAvailableNow


SCHEMAS = {
    "cases": ("case_id string, folder string, schema_version int, opened timestamp, status string, "
              "priority int, tags array<string>, meta variant, attachments int, bytes long, "
              "problems array<string>"),
    "attachments": ("case_id string, folder string, name string, kind string, size long, "
                    "sha256 string, expected_sha256 string, status string"),
    "changes": ("case_id string, line int, ts timestamp, field string, old string, new string, "
                "actor string, raw string, problem string"),
}
MANIFEST = "_manifest.json"
KINDS = {".pdf": "pdf", ".png": "image", ".wav": "audio", ".eml": "email",
         ".txt": "text", ".json": "json"}
CHANGE_LINE = re.compile(r"^(\S+) \| (\w+) \| (.*?) -> (.*?) \| (\S+)$")


def get_log():
    """The connector's logger. Call it where you log: a Logger pickles by name
    only, so a handler added at import time on the driver is not there in a
    worker. The NullHandler keeps warnings off stderr when nothing collects
    them; with worker logging on, python_worker_logs() still gets them."""
    logger = logging.getLogger("casefile")
    if not logger.handlers:
        logger.addHandler(logging.NullHandler())
    return logger


def columns_of(table):
    """Column names of a table, from its DDL string (no Spark needed)."""
    return [field.split()[0] for field in SCHEMAS[table].split(", ")]


# =====================================================================
# Stage "parsing": plain functions (no Spark)
# =====================================================================
def parse_ts(text):
    """An ISO 8601 timestamp ('Z' or an offset) as an aware UTC datetime, or None."""
    if not isinstance(text, str):
        return None
    try:
        ts = datetime.datetime.fromisoformat(text.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if ts.tzinfo is None:
        return None
    return ts.astimezone(datetime.timezone.utc)


def kind_of(name):
    return KINDS.get(os.path.splitext(name)[1].lower(), "other")


def parse_priority(value):
    """'P1'..'P4' or 1..4 -> (int, None); missing -> (None, None); anything else -> (None, 'bad_priority')."""
    if value is None:
        return None, None
    if isinstance(value, bool):
        return None, "bad_priority"
    if isinstance(value, int) and 1 <= value <= 4:
        return value, None
    if isinstance(value, str) and re.fullmatch(r"[Pp][1-4]", value.strip()):
        return int(value.strip()[1]), None
    return None, "bad_priority"


def parse_tags(value):
    if value is None:
        return []
    if isinstance(value, str):
        return [t.strip() for t in value.split(",") if t.strip()]
    return [str(t) for t in value]


def parse_index(raw, folder):
    """Normalize the bytes of a case.json (schema 1 or 2) into one dict.

    Keys: case_id, folder, schema_version, opened, status, priority, tags,
    meta (a dict or None), listed (list of {"name", "sha256", "bytes"};
    sha256 and bytes are None for schema 1), problems (sorted list).
    """
    fallback = folder.split("@")[0].upper()
    base = {"case_id": fallback, "folder": folder, "schema_version": None, "opened": None,
            "status": None, "priority": None, "tags": [], "meta": None, "listed": [], "problems": []}
    try:
        doc = json.loads(raw.decode("utf-8-sig"))        # utf-8-sig drops a byte order mark
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {**base, "problems": ["bad_index"]}
    if not isinstance(doc, dict):
        return {**base, "problems": ["bad_index"]}

    problems = []
    if doc.get("schema") == 2:
        version, case_id, opened = 2, doc.get("case_id"), parse_ts(doc.get("opened"))
        status, prio, tags, meta = doc.get("status"), doc.get("priority"), doc.get("tags"), doc.get("meta")
        listed = [{"name": a["name"], "sha256": a.get("sha256"), "bytes": a.get("bytes")}
                  for a in doc.get("attachments") or []]
    elif "schema" not in doc and "id" in doc:
        version, case_id = 1, doc.get("id")
        created = doc.get("created")
        opened = (datetime.datetime.fromtimestamp(created, datetime.timezone.utc)
                  if isinstance(created, (int, float)) and not isinstance(created, bool) else None)
        status, prio, tags, meta = doc.get("state"), doc.get("prio"), doc.get("labels"), doc.get("extra")
        listed = [{"name": n, "sha256": None, "bytes": None} for n in doc.get("attachments") or []]
    else:
        return {**base, "problems": ["bad_index"]}

    priority, problem = parse_priority(prio)
    if problem:
        problems.append(problem)
    case_id = str(case_id).upper() if case_id else fallback
    if case_id != fallback:
        problems.append("id_mismatch")
    return {"case_id": case_id, "folder": folder, "schema_version": version, "opened": opened,
            "status": status.lower() if isinstance(status, str) else None, "priority": priority,
            "tags": parse_tags(tags), "meta": meta if isinstance(meta, dict) else None,
            "listed": listed, "problems": sorted(problems)}


def parse_change_line(line):
    """One changes.log line (without its newline) -> dict, or None if it is not a change."""
    line = line.rstrip("\r")
    m = CHANGE_LINE.match(line.strip())
    if not m:
        return None
    ts = parse_ts(m.group(1))
    if ts is None:
        return None
    return {"ts": ts, "field": m.group(2), "old": m.group(3).strip(), "new": m.group(4).strip(),
            "actor": m.group(5)}


def change_rows(case_id, text, first_line):
    """Rows of the changes table for `text` (whole lines), numbering from first_line.

    Blank lines and comments (#) produce no row; lines that are not changes
    produce a row with raw set and problem 'unparsed'.
    """
    for n, line in enumerate(text.split("\n"), first_line):
        line = line.rstrip("\r").lstrip("\ufeff")
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        c = parse_change_line(line)
        if c is None:
            yield (case_id, n, None, None, None, None, None, line, "unparsed")
        else:
            yield (case_id, n, c["ts"], c["field"], c["old"], c["new"], c["actor"], None, None)


def attachment_status(expected_sha, actual_sha, listed, on_disk):
    """'ok', 'sha_mismatch', 'unverified' (no checksum), 'missing' or 'unlisted'."""
    if listed and not on_disk:
        return "missing"
    if on_disk and not listed:
        return "unlisted"
    if expected_sha is None:
        return "unverified"
    return "ok" if expected_sha == actual_sha else "sha_mismatch"


# =====================================================================
# Stage "planning": partitions by bytes
# =====================================================================
def plan_by_bytes(items, target):
    """Group (key, size) items so each group's total is at most target bytes.

    First fit decreasing: largest first (ties by key), each into the first
    group that still has room. An item bigger than target gets a group of
    its own. Items are never split.
    """
    groups, totals = [], []
    for key, size in sorted(items, key=lambda kv: (-kv[1], kv[0])):
        for i, total in enumerate(totals):
            if total + size <= target:
                groups[i].append(key)
                totals[i] += size
                break
        else:
            groups.append([key])
            totals.append(size)
    return groups


def list_case_folders(root):
    """The case folders under root, sorted.

    With a manifest, exactly the folders it lists (so folders of a write
    that never committed stay invisible); without one, every folder that
    has a case.json, skipping names that start with '_' or '.' and job
    folders (name@job) that no manifest has published.
    """
    path = os.path.join(root, MANIFEST)
    if os.path.exists(path):
        with open(path) as f:
            return sorted(json.load(f)["cases"].values())
    return sorted(e.name for e in os.scandir(root)
                  if e.is_dir() and not e.name.startswith(("_", ".")) and "@" not in e.name
                  and os.path.exists(os.path.join(e.path, "case.json")))


def read_index(root, folder):
    with open(os.path.join(root, folder, "case.json"), "rb") as f:
        return parse_index(f.read(), folder)


# =====================================================================
# Stage "batch": the source and its readers
# =====================================================================
class CasePartition(InputPartition):
    def __init__(self, items):
        self.items = items     # case folders, or (folder, name) pairs for attachments


class CaseFileSource(DataSource):
    @classmethod
    def name(cls):
        return "casefile"

    def schema(self):
        table = self.options.get("table", "cases")
        if table not in SCHEMAS:
            raise ValueError(f"casefile: option 'table' must be one of {sorted(SCHEMAS)}, got {table!r}")
        return SCHEMAS[table]

    def reader(self, schema):
        # Two classes on purpose: a reader that defines pushFilters fails every
        # query when spark.sql.python.filterPushdown.enabled is false, and on
        # 4.2.0 pushdown can leak into other scans of a reused DataFrame. So
        # pushdown is opt-in, and the default reader cannot leak.
        if self.options.get("pushdown", "false").lower() == "true":
            return PushdownCaseFileReader(self.options, schema)
        return CaseFileReader(self.options, schema)

    def writer(self, schema, overwrite):
        if self.options.get("table", "cases") != "cases":
            raise ValueError("casefile can only write the cases table")
        return CaseFileWriter(self.options, overwrite)

    def streamReader(self, schema):
        if self.options.get("table", "cases") != "changes":
            raise ValueError("casefile can only stream the changes table (option table=changes)")
        return ChangesStreamReader(self.options)


class CaseFileReader(DataSourceReader):
    def __init__(self, options, schema):
        if "path" not in options:
            raise ValueError("casefile needs a folder: .load(path)")
        self.root = os.path.abspath(options["path"])
        if not os.path.isdir(self.root):
            raise ValueError(f"casefile: {self.root} is not a folder")
        self.table = options.get("table", "cases")
        self.target = int(options.get("maxPartitionBytes", 16384))
        self.audit = options.get("audit")
        # The schema Spark passes (a StructType), or None in tests without Spark
        self.columns = [f.name for f in schema.fields] if schema is not None else columns_of(self.table)
        self.case_ids = None    # set of case ids, or None for all (pushdown)
        self.statuses = None
        self.opened_tests = []  # (operator name, aware datetime)

    # -- planning, on the driver --
    def wanted(self, folder):
        """Does a folder pass the pushed filters? Reads the index only if it must."""
        if self.case_ids is not None and folder.split("@")[0].upper() not in self.case_ids:
            return False
        if self.statuses is None and not self.opened_tests:
            return True
        index = read_index(self.root, folder)
        if self.statuses is not None and index["status"] not in self.statuses:
            return False
        ops = {"GreaterThan": lambda a, b: a > b, "GreaterThanOrEqual": lambda a, b: a >= b,
               "LessThan": lambda a, b: a < b, "LessThanOrEqual": lambda a, b: a <= b}
        return all(index["opened"] is not None and ops[op](index["opened"], value)
                   for op, value in self.opened_tests)

    def partitions(self):
        folders = [f for f in list_case_folders(self.root) if self.wanted(f)]
        if self.table == "attachments":
            items = []
            for folder in folders:
                d = os.path.join(self.root, folder, "attachments")
                names = sorted(os.listdir(d)) if os.path.isdir(d) else []
                items += [((folder, n), os.path.getsize(os.path.join(d, n))) for n in names]
                if not names:
                    items.append(((folder, None), 0))   # a case with nothing on disk still has listed rows
        else:
            name = "case.json" if self.table == "cases" else "changes.log"
            items = [(folder, os.path.getsize(os.path.join(self.root, folder, name))
                      if os.path.exists(os.path.join(self.root, folder, name)) else 0)
                     for folder in folders]
        return [CasePartition(g) for g in plan_by_bytes(items, self.target)]

    # -- reading, on executors --
    def touch(self, folder):
        if self.audit:
            with open(os.path.join(self.audit, f"open-{self.table}-{folder}-{uuid.uuid4().hex}"), "w"):
                pass

    def read(self, partition):
        if not isinstance(partition, CasePartition):
            return
        if self.table == "cases":
            rows = (self.case_row(folder) for folder in partition.items)
        elif self.table == "attachments":
            rows = self.attachment_rows(partition.items)
        else:
            rows = (r for folder in partition.items for r in self.change_rows_of(folder))
        for row in rows:
            yield tuple(row[c] for c in self.columns)

    def case_row(self, folder):
        from pyspark.sql.types import VariantVal

        self.touch(folder)
        index = read_index(self.root, folder)
        d = os.path.join(self.root, folder, "attachments")
        on_disk = sorted(os.listdir(d)) if os.path.isdir(d) else []
        listed = {a["name"] for a in index["listed"]}
        problems = set(index["problems"])
        if listed - set(on_disk):
            problems.add("missing_attachment")
        if set(on_disk) - listed and "bad_index" not in problems:
            problems.add("unlisted_attachment")
        for p in sorted(problems):
            get_log().warning("%s %s", index["case_id"], p)
        return {"case_id": index["case_id"], "folder": folder, "schema_version": index["schema_version"],
                "opened": index["opened"], "status": index["status"], "priority": index["priority"],
                "tags": index["tags"],
                "meta": VariantVal.parseJson(json.dumps(index["meta"])) if index["meta"] is not None else None,
                "attachments": len(index["listed"]),
                "bytes": sum(os.path.getsize(os.path.join(d, n)) for n in on_disk),
                "problems": sorted(problems)}

    def attachment_rows(self, items):
        import hashlib

        by_folder = {}
        for folder, name in items:
            by_folder.setdefault(folder, []).append(name)
        for folder, names in by_folder.items():
            self.touch(folder)
            index = read_index(self.root, folder)
            listed = {a["name"]: a for a in index["listed"]}
            d = os.path.join(self.root, folder, "attachments")
            on_disk = set(os.listdir(d)) if os.path.isdir(d) else set()
            # This partition owns the files it was given; the partition that
            # holds the folder's first planned item also owns the missing ones.
            first = min(on_disk) if on_disk else None
            mine = [n for n in names if n is not None]
            if first is None or first in mine:
                mine += sorted(set(listed) - on_disk)
            for name in sorted(mine):
                exists = name in on_disk
                actual = size = None
                if exists:
                    with open(os.path.join(d, name), "rb") as f:
                        data = f.read()
                    actual, size = hashlib.sha256(data).hexdigest(), len(data)
                expected = listed[name]["sha256"] if name in listed else None
                status = attachment_status(expected, actual, name in listed, exists)
                if status not in ("ok", "unverified"):
                    get_log().warning("%s %s %s", index["case_id"], status, name)
                yield {"case_id": index["case_id"], "folder": folder, "name": name, "kind": kind_of(name),
                       "size": size, "sha256": actual, "expected_sha256": expected, "status": status}

    def change_rows_of(self, folder):
        self.touch(folder)
        path = os.path.join(self.root, folder, "changes.log")
        if not os.path.exists(path):
            return
        with open(path, "rb") as f:
            text = f.read().decode("utf-8")
        case_id = folder.split("@")[0].upper()
        for row in change_rows(case_id, text, 1):
            yield dict(zip(("case_id", "line", "ts", "field", "old", "new", "actor", "raw", "problem"), row))


# =====================================================================
# Stage "pushdown": opt-in, exact, and safe to reuse
# =====================================================================
class PushdownCaseFileReader(CaseFileReader):
    def pushFilters(self, filters):
        for f in filters:
            column = f.attribute if hasattr(f, "attribute") else None
            if column == ("case_id",) and isinstance(f, (EqualTo, In)):
                values = {f.value} if isinstance(f, EqualTo) else set(f.value)
                self.case_ids = values if self.case_ids is None else self.case_ids & values
            elif self.table == "cases" and column == ("status",) and isinstance(f, (EqualTo, In)):
                values = {f.value} if isinstance(f, EqualTo) else set(f.value)
                self.statuses = values if self.statuses is None else self.statuses & values
            elif (self.table == "cases" and column == ("opened",)
                  and isinstance(f, (GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual))):
                self.opened_tests.append((type(f).__name__, f.value))
            else:
                yield f


class CaseFiles:
    """The safe way to use casefile with pushdown on Spark 4.1 to 4.3.

    Every table() call is a fresh load(), so no two scans share a provider
    (see "From Wrong Results to a Fix"). Call it once per reference to a
    table, even inside one query.
    """

    def __init__(self, spark, path, **options):
        self.spark, self.path, self.options = spark, os.path.abspath(path), options

    def table(self, name, **options):
        reader = self.spark.read.format("casefile").option("table", name)
        for k, v in {**self.options, **options}.items():
            reader = reader.option(k, v)
        return reader.load(self.path)


# =====================================================================
# Stage "writer": invisible until one atomic manifest swap
# =====================================================================
def read_manifest(root):
    path = os.path.join(root, MANIFEST)
    if not os.path.exists(path):
        return {"version": 0, "cases": {}, "jobs": []}
    with open(path) as f:
        return json.load(f)


class Written(WriterCommitMessage):
    def __init__(self, folders):
        self.folders = folders    # [(case_id, folder name)]


class CaseFileWriter(DataSourceWriter):
    def __init__(self, options, overwrite):
        if "path" not in options:
            raise ValueError("casefile needs a folder to write to: .save(path)")
        self.root = os.path.abspath(options["path"])
        self.overwrite = overwrite
        self.job = uuid.uuid4().hex[:8]

    def write(self, rows):
        """Each task writes case folders named <case_id>@<job>: invisible until commit.

        A case_id that is not CASE-dddd (any case) raises ValueError("... invalid case_id ...").
        """
        os.makedirs(self.root, exist_ok=True)
        written = []
        for row in rows:
            r = row.asDict()
            case_id = str(r["case_id"]).upper()
            if not re.fullmatch(r"CASE-\d{4}", case_id):
                raise ValueError(f"casefile: invalid case_id {r['case_id']!r}, expected CASE-dddd")
            folder = f"{case_id}@{self.job}"
            d = os.path.join(self.root, folder)
            os.makedirs(os.path.join(d, "attachments"), exist_ok=True)
            opened = r.get("opened")
            meta = r.get("meta")
            index = {"schema": 2, "case_id": case_id,
                     "opened": opened.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                     if opened is not None else None,
                     "status": r.get("status"),
                     "priority": f"P{r['priority']}" if r.get("priority") is not None else None,
                     "tags": list(r.get("tags") or []),
                     "meta": json.loads(meta.toJson()) if meta is not None else None,
                     "attachments": []}
            with open(os.path.join(d, "case.json"), "w") as f:
                json.dump(index, f, indent=2, ensure_ascii=False)
            open(os.path.join(d, "changes.log"), "w").close()
            written.append((case_id, folder))
        return Written(written)

    def commit(self, messages):
        written = [w for m in messages for w in m.folders]
        ids = [case_id for case_id, _ in written]
        dupes = sorted({i for i in ids if ids.count(i) > 1})
        if dupes:
            raise ValueError(f"casefile: duplicate case_id in this write: {dupes}")
        old = read_manifest(self.root)
        if not old["cases"] and not os.path.exists(os.path.join(self.root, MANIFEST)):
            # First write into a folder that may hold a plain export: adopt it.
            old["cases"] = {} if self.overwrite else {
                f.split("@")[0].upper(): f for f in list_case_folders(self.root)}
        clash = sorted(set(ids) & set(old["cases"])) if not self.overwrite else []
        if clash:
            raise ValueError(f"casefile: case_id already exists (use overwrite): {clash}")
        cases = {} if self.overwrite else dict(old["cases"])
        cases.update(dict(written))
        new = {"version": old["version"] + 1, "cases": cases,
               "jobs": old["jobs"] + [{"job": self.job, "mode": "overwrite" if self.overwrite else "append",
                                       "cases": len(written)}]}
        tmp = os.path.join(self.root, f".{MANIFEST}.{self.job}")
        with open(tmp, "w") as f:
            json.dump(new, f, indent=1)
        os.replace(tmp, os.path.join(self.root, MANIFEST))      # the commit point
        # Only now delete folders the new manifest no longer points to.
        for folder in set(old["cases"].values()) - set(cases.values()):
            shutil.rmtree(os.path.join(self.root, folder), ignore_errors=True)

    def abort(self, messages):
        for entry in os.listdir(self.root) if os.path.isdir(self.root) else []:
            if entry.endswith("@" + self.job):
                shutil.rmtree(os.path.join(self.root, entry), ignore_errors=True)


# =====================================================================
# Stage "streaming": the changes table as a stream
# =====================================================================
class ChangesRange(InputPartition):
    def __init__(self, folder, start, end, first_line):
        self.folder, self.start, self.end, self.first_line = folder, start, end, first_line


def complete_lines(path, start):
    """[(end byte, newline count)] for each complete line after byte `start`."""
    with open(path, "rb") as f:
        f.seek(start)
        data = f.read()
    out, pos = [], start
    for piece in data.split(b"\n")[:-1]:          # the last piece has no newline yet
        pos += len(piece) + 1
        out.append(pos)
    return out


class ChangesStreamReader(DataSourceStreamReader, SupportsTriggerAvailableNow):
    """Offsets: {"v": 1, "pos": {folder: [byte, lines]}}, bytes and lines consumed per log."""

    def __init__(self, options):
        self.root = os.path.abspath(options["path"])
        self.max_bytes = int(options.get("maxBytesPerTrigger", 1024))
        self.target = None

    def initialOffset(self):
        return {"v": 1, "pos": {}}

    def getDefaultReadLimit(self):
        return ReadMaxBytes(self.max_bytes)

    def pending(self, start):
        """{folder: [end byte of each complete unread line]}, in folder order."""
        out = {}
        for folder in list_case_folders(self.root):
            path = os.path.join(self.root, folder, "changes.log")
            if not os.path.exists(path):
                continue
            begin = start["pos"].get(folder, [0, 0])[0]
            ends = complete_lines(path, begin)
            if self.target is not None:
                cap = self.target.get(folder, 0)
                ends = [e for e in ends if e <= cap]
            if ends:
                out[folder] = ends
        return out

    def prepareForTriggerAvailableNow(self):
        everything = self.pending({"v": 1, "pos": {}})
        self.target = {folder: ends[-1] for folder, ends in everything.items()}

    def latestOffset(self, start, limit):
        budget = None if isinstance(limit, ReadAllAvailable) else getattr(limit, "max_bytes", None)
        pos = {k: list(v) for k, v in start["pos"].items()}
        used, took = 0, False
        for folder, ends in self.pending(start).items():
            begin, lines = pos.get(folder, [0, 0])
            for end in ends:
                size = end - begin
                if budget is not None and took and used + size > budget:
                    return {"v": 1, "pos": pos}
                used += size
                took = True
                begin, lines = end, lines + 1
                pos[folder] = [begin, lines]
        return {"v": 1, "pos": pos}

    def partitions(self, start, end):
        parts = []
        for folder, (stop, _) in sorted(end["pos"].items()):
            begin, lines = start["pos"].get(folder, [0, 0])
            if stop > begin:
                parts.append(ChangesRange(folder, begin, stop, lines + 1))
        return parts

    def read(self, partition):
        with open(os.path.join(self.root, partition.folder, "changes.log"), "rb") as f:
            f.seek(partition.start)
            text = f.read(partition.end - partition.start).decode("utf-8")
        text = text[:-1] if text.endswith("\n") else text      # the range ends with a newline
        yield from change_rows(partition.folder.split("@")[0].upper(), text, partition.first_line)

    def commit(self, end):
        pass


# =====================================================================
# Stage "offline_tests": tests that need no Spark
# =====================================================================


# The workers do not have this file on their path: ship the module's code with the pickled
# classes. A packaged source (Chapter 83) installs the module on every worker instead.
from pyspark import cloudpickle  # noqa: E402
cloudpickle.register_pickle_by_value(sys.modules[__name__])

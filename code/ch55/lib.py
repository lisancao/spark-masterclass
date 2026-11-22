"""Shared pieces for Chapter 55: the jar, a fixed copy of the messy folder, and the Python docs source."""
import datetime, os, shutil, struct, sys, tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
from common.book import ROOT  # noqa: E402

JAR = HERE / "jdocs" / "target" / "jdocs-0.1.0.jar"
MESSY = ROOT / "code" / "data" / "messy"
FOLDERS = ["audio", "docs", "events", "images", "logs", "mail", "pdf", "xml"]   # fixed, so counts are stable


def docs_root():
    """A copy of the messy folders jdocs reads, at a fixed path (copy2 keeps modification times)."""
    root = Path(tempfile.gettempdir()) / "masterclass-jdocs"
    shutil.rmtree(root, ignore_errors=True)
    for d in FOLDERS:
        shutil.copytree(MESSY / d, root / d, copy_function=shutil.copy2)
    return str(root)


def short(path_text):
    """Hide the temporary folder in printed paths."""
    return path_text.replace(str(Path(tempfile.gettempdir()) / "masterclass-jdocs"), "<root>")


KINDS = {".png": "image", ".wav": "audio", ".pdf": "pdf", ".eml": "email", ".md": "text",
         ".txt": "text", ".log": "log", ".json": "json", ".xml": "xml"}
SCHEMA = "path string, name string, kind string, size long, modified timestamp, width int, height int"


def kind_of(name):
    return KINDS.get(os.path.splitext(name)[1].lower(), "other")


def py_rows(root):
    """The rows of the docs source, computed in plain Python: the expected answer."""
    rows = []
    for folder, _, names in os.walk(root):
        for n in names:
            p = os.path.abspath(os.path.join(folder, n))
            st = os.stat(p)
            w = h = None
            if kind_of(n) == "image":
                with open(p, "rb") as f:
                    head = f.read(24)
                if len(head) == 24 and head[:8] == b"\x89PNG\r\n\x1a\n" and head[12:16] == b"IHDR":
                    w, h = struct.unpack(">II", head[16:24])
            modified = (datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
                        + datetime.timedelta(microseconds=st.st_mtime_ns // 1000))
            rows.append((p, n, kind_of(n), st.st_size, modified, w, h))
    return sorted(rows)


def make_pydocs(pushdown=False):
    """The docs source as a Python data source. With pushdown=True it takes kind = '...' filters."""
    from pyspark.sql.datasource import DataSource, DataSourceReader, EqualTo
    from pyspark import cloudpickle
    cloudpickle.register_pickle_by_value(sys.modules[__name__])   # workers do not have lib.py on their path

    class PyDocsReader(DataSourceReader):
        def __init__(self, root):
            self.root, self.kinds = root, None

        def read(self, partition):
            for r in py_rows(self.root):
                if self.kinds is None or r[2] in self.kinds:
                    yield r

    class PyDocsPushReader(PyDocsReader):
        def pushFilters(self, filters):
            for f in filters:
                if isinstance(f, EqualTo) and f.attribute == ("kind",):
                    self.kinds = {f.value}
                else:
                    yield f      # not taken: Spark keeps it

    class PyDocs(DataSource):
        @classmethod
        def name(cls):
            return "pydocs"

        def schema(self):
            return SCHEMA

        def reader(self, schema):
            return (PyDocsPushReader if pushdown else PyDocsReader)(self.options["path"])

    return PyDocs

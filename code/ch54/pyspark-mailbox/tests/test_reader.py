"""Plain-Python tests for the mailbox reader: no SparkSession, no JVM, the same checks Spark makes."""
import os
import shutil

import pytest
from pyspark import cloudpickle
from pyspark.sql.datasource import CaseInsensitiveDict, InputPartition

from pyspark_mailbox import DefaultSource, MailboxDataSource

MAIL = os.environ.get("MAIL_DIR", os.path.join(os.path.dirname(__file__), "..", "..", "..", "data",
                                               "messy", "mail"))


def read_without_spark(options, ncols=4):
    """Plan and read like Spark: a list of InputPartition, a pickle round trip, rows of ncols values."""
    reader = MailboxDataSource(CaseInsensitiveDict(options)).reader(None)
    parts = reader.partitions()
    assert isinstance(parts, list), f"partitions() must return a list, got {type(parts).__name__}"
    assert all(isinstance(p, InputPartition) for p in parts), "every partition must be an InputPartition"
    reader, parts = cloudpickle.loads(cloudpickle.dumps((reader, parts)))
    rows = [tuple(r) for p in parts for r in reader.read(p)]
    assert all(len(r) == ncols for r in rows), "every row needs one value per column"
    return parts, rows


def test_default_source_is_the_source():
    assert DefaultSource is MailboxDataSource and DefaultSource.name() == "mailbox"


def test_one_row_per_email():
    parts, rows = read_without_spark({"path": MAIL, "Partitions": "3"})
    assert len(parts) == 3 and len(rows) == 4


def test_more_partitions_than_files():
    parts, rows = read_without_spark({"path": MAIL, "partitions": "10"})
    assert len(parts) == 4 and len(rows) == 4


def test_missing_path_is_a_clear_error():
    with pytest.raises(ValueError, match="needs a folder"):
        MailboxDataSource(CaseInsensitiveDict({})).reader(None)


def test_broken_file_names_the_file(tmp_path):
    shutil.copy(os.path.join(MAIL, "ticket_0.eml"), tmp_path)
    (tmp_path / "broken.eml").write_text("Subject: no sender\n\nhello\n")
    with pytest.raises(ValueError, match="broken.eml has no From header"):
        read_without_spark({"path": str(tmp_path), "partitions": "1"})

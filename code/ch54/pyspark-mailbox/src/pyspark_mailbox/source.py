"""format("mailbox"): one row per .eml file in the folder given as path."""
import logging
import os

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

SCHEMA = "file string, sender string, subject string, body string"
log = logging.getLogger("mailbox")


class MailPartition(InputPartition):
    def __init__(self, files):
        self.files = files


class MailboxDataSource(DataSource):
    @classmethod
    def name(cls):
        return "mailbox"

    def schema(self):
        return SCHEMA

    def reader(self, schema):
        return MailboxReader(self.options)


class MailboxReader(DataSourceReader):
    def __init__(self, options):
        if "path" not in options:
            raise ValueError("mailbox needs a folder: .load(path)")
        self.root = os.path.abspath(options["path"])
        self.n = int(options.get("partitions", "2"))

    def partitions(self):
        files = sorted(f for f in os.listdir(self.root) if f.endswith(".eml"))
        return [MailPartition(files[i::self.n]) for i in range(self.n) if files[i::self.n]]

    def read(self, partition):
        from email import policy
        from email.parser import BytesParser

        log.setLevel(logging.INFO)                  # the default level is WARNING: info() is dropped
        for name in partition.files:
            with open(os.path.join(self.root, name), "rb") as f:
                msg = BytesParser(policy=policy.default).parse(f)
            if msg["From"] is None:
                raise ValueError(f"{name} has no From header")
            log.info("parsed %s", name)
            body = msg.get_body(("plain",)).get_content().strip()
            yield (name, str(msg["From"]), str(msg["Subject"]), body)

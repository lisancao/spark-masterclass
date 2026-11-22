"""pyspark_mailbox: Spark imports every top-level pyspark_* module and registers its DefaultSource."""
from pyspark_mailbox.source import MailboxDataSource

DefaultSource = MailboxDataSource

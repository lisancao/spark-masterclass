"""Helpers for Chapter 43: start a fresh Classic session with a chosen master.

builder.getOrCreate() silently returns the session that is already running, and the master
(local[4] versus local[4,2]) is fixed when the JVM-side SparkContext starts, so every experiment
that changes the master stops the old session first.
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.book import ui_port  # noqa: E402


def fresh(master="local[4]", **conf):
    from pyspark.sql import SparkSession
    active = SparkSession.getActiveSession()
    if active is not None:
        active.stop()
    b = (SparkSession.builder.master(master).appName("ch43")
         .config("spark.ui.port", ui_port())
         .config("spark.sql.session.timeZone", "UTC"))
    for k, v in conf.items():
        b = b.config(k, v)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
    return spark


def first_line(e, width=88):
    """The first line of an exception message, trimmed to fit a terminal."""
    line = str(e).strip().splitlines()[0]
    return line if len(line) <= width else line[: width - 3] + "..."

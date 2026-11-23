"""Small helpers for Chapter 19: the taxi views, plan node lines and SQL metrics from the UI REST API."""
import json
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.book import session, plan, TRIPS, ZONES, MONTH_START, MONTH_END  # noqa: E402


def start(app, **conf):
    """A session with the June trips as `trips`, the zones as `zones`, and the cleaned join as `june`."""
    conf.setdefault("spark_sql_session_timeZone", "UTC")
    conf.setdefault("spark_ui_showConsoleProgress", "false")
    spark = session(app, **conf)
    spark.read.parquet(str(TRIPS)).createOrReplaceTempView("trips")
    (spark.read.option("header", True).option("inferSchema", True).csv(str(ZONES))
     .createOrReplaceTempView("zones"))
    spark.sql(f"""CREATE OR REPLACE TEMP VIEW june AS
    SELECT t.*, z.Borough, z.Zone FROM trips t JOIN zones z ON t.PULocationID = z.LocationID
    WHERE tpep_pickup_datetime >= '{MONTH_START}' AND tpep_pickup_datetime < '{MONTH_END}'
      AND fare_amount > 0 AND trip_distance BETWEEN 0.1 AND 100""")
    return spark


def nodes(df, final=False):
    """Operator lines of the physical plan, one per node, without IDs after '#'."""
    import re
    text = plan(df, "simple")
    if final:
        text = text.split("== Initial Plan ==")[0]
    out = []
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith("==") or s.startswith("AdaptiveSparkPlan"):
            continue
        s = re.sub(r"#\d+L?", "", s)
        s = re.sub(r", \[plan_id=\d+\]", "", s)
        out.append(line[: len(line) - len(line.lstrip())] + s)
    return out


def short(line, width=88):
    return line if len(line) <= width else line[: width - 3] + "..."


def sql_metrics(spark, wanted=("number of output rows",)):
    """(node name, description, {metric: value}) for each node of the last SQL execution."""
    base = spark.sparkContext.uiWebUrl
    app = json.load(urllib.request.urlopen(base + "/api/v1/applications"))[0]["id"]
    url = f"{base}/api/v1/applications/{app}/sql?details=true&planDescription=false&length=10000"
    ex = json.load(urllib.request.urlopen(url))[-1]
    out = []
    for n in ex["nodes"]:
        m = {x["name"]: x["value"] for x in n.get("metrics", []) if x["name"] in wanted}
        out.append((n["nodeName"], n.get("desc", ""), m))
    return out


def row_counts(spark):
    """The 'number of output rows' of the scan, Expand, Filter and aggregate nodes of the last query."""
    keep = ("Scan", "HashAggregate", "ObjectHashAggregate", "SortAggregate", "Expand", "Filter", "Generate")
    lines = []
    for name, desc, m in sql_metrics(spark, ("number of output rows",)):
        if name.startswith(keep) and "number of output rows" in m:
            lines.append((name, desc, int(m["number of output rows"].replace(",", ""))))
    return lines

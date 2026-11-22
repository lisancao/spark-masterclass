"""Chapter 73 figures: a timeline of Spark releases (facts from the release notes) and a map of the modules.

The timeline rows are the same facts as the table in the chapter; each one has its release-notes link there.
Run from code/:  uv run python ch73/figures.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.svgfig import write, text, rect, arrow

# (year as a float, label, line 1, line 2, lane) ; lane: core, sql, stream
EVENTS = [
    (2010.5, "paper", "Spark: working sets", "HotCloud 2010", "core"),
    (2012.3, "paper", "RDDs", "NSDI 2012", "core"),
    (2013.1, "0.7", "PySpark, Spark", "Streaming (alpha)", "stream"),
    (2014.4, "1.0", "Spark SQL, MLlib,", "GraphX, Streaming", "sql"),
    (2015.2, "1.3", "DataFrames", "", "sql"),
    (2015.7, "1.5", "Tungsten on", "by default", "core"),
    (2016.0, "1.6", "Datasets, unified", "memory", "core"),
    (2016.6, "2.0", "Structured Streaming", "(experimental), codegen", "stream"),
    (2017.5, "2.2", "Structured Streaming", "GA", "stream"),
    (2018.2, "2.3", "continuous processing,", "Kubernetes (experimental)", "core"),
    (2020.5, "3.0", "adaptive query", "execution", "sql"),
    (2021.8, "3.2", "pandas API on Spark,", "RocksDB state store", "sql"),
    (2023.3, "3.4", "Spark Connect Python", "client, DStream deprecated", "stream"),
    (2025.4, "4.0", "ANSI by default, VARIANT,", "Python data sources", "sql"),
    (2025.95, "4.1", "Declarative Pipelines,", "Real-time Mode", "stream"),
    (2026.5, "4.2", "GEOMETRY, GEOGRAPHY,", "Auto CDC (SCD 1)", "sql"),
    (2026.83, "4.3", "stateful Real-time Mode,", "Project Feather", "stream"),
]


def timeline(c):
    W, L, R, Y = 1200, 70, 1090, 260
    x = lambda yr: L + (yr - 2010) / (2027 - 2010) * (R - L)
    lane = {"core": c["acc"], "sql": c["blue"], "stream": c["green"]}
    out = [text(20, 26, "Spark from the first paper to 4.3 (dates and features from the release notes)", c, 16, c["fg"], 700, "start")]
    out.append(f'<line x1="{L}" y1="{Y}" x2="{R}" y2="{Y}" stroke="{c["line"]}" stroke-width="2"/>')
    for yr in range(2010, 2027, 2):
        out.append(f'<line x1="{x(yr)}" y1="{Y - 5}" x2="{x(yr)}" y2="{Y + 5}" stroke="{c["line"]}" stroke-width="1.5"/>')
        out.append(text(x(yr), Y + 22, str(yr), c, 12, c["mute"]))
    up = True
    levels = [70, 150]
    for i, (yr, lab, l1, l2, ln) in enumerate(EVENTS):
        col = lane[ln]
        lvl = levels[(i // 2) % 2]
        yy = Y - lvl if up else Y + lvl - 10
        out.append(f'<line x1="{x(yr)}" y1="{Y}" x2="{x(yr)}" y2="{yy + (18 if up else -32)}" stroke="{col}" stroke-width="1.2"/>')
        out.append(f'<circle cx="{x(yr)}" cy="{Y}" r="5" fill="{col}"/>')
        out.append(text(x(yr), yy - 16, lab, c, 13, col, 700))
        out.append(text(x(yr), yy, l1, c, 11, c["fg"]))
        if l2:
            out.append(text(x(yr), yy + 13, l2, c, 11, c["fg"]))
        up = not up
    for k, (ln, name) in enumerate((("core", "core engine"), ("sql", "SQL and APIs"), ("stream", "streaming and connectivity"))):
        out.append(f'<circle cx="{L + 10 + k * 220}" cy="490" r="6" fill="{lane[ln]}"/>')
        out.append(text(L + 22 + k * 220, 494, name, c, 12, c["fg"], 500, "start"))
    return out


def modules(c):
    out = [text(20, 26, "The modules of Spark 4.3, and the chapters of this book that teach them", c, 16, c["fg"], 700, "start")]
    # clients
    out.append(rect(20, 45, 1060, 60, c, fill=c["purple_soft"], stroke=c["purple"], r=10))
    out.append(text(40, 70, "Your code", c, 14, c["purple"], 700, "start"))
    for i, (n, ch) in enumerate((("PySpark", "ch 1-4"), ("Scala / Java", ""), ("SQL", "ch 7-8"), ("Spark Connect clients", "ch 4, 46, 57"))):
        out.append(text(250 + i * 210, 70, n, c, 13, c["fg"], 600))
        out.append(text(250 + i * 210, 90, ch, c, 11, c["mute"]))
    # APIs on the SQL engine
    apis = [("DataFrame / Dataset / SQL", "ch 5-17"), ("Structured Streaming", "ch 25-29"), ("MLlib (spark.ml)", "ch 37-40"),
            ("pandas API on Spark", "not covered"), ("Declarative Pipelines", "ch 32-36"), ("GraphFrames (package)", "ch 73")]
    for i, (n, ch) in enumerate(apis):
        x = 20 + i * 178
        dash = "5 4" if "package" in n else None
        out.append(rect(x, 125, 168, 64, c, fill=c["blue_soft"], stroke=c["blue"], r=8, dash=dash))
        out.append(text(x + 84, 152, n, c, 12, c["fg"], 600))
        out.append(text(x + 84, 172, ch, c, 11, c["mute"]))
    out.append(rect(20, 205, 1060, 60, c, fill=c["acc_soft"], stroke=c["acc"], r=10))
    out.append(text(550, 230, "Spark SQL engine: Catalyst, Tungsten, codegen, AQE, data sources, Feather", c, 14, c["fg"], 700))
    out.append(text(550, 250, "ch 18-24, 72, extension points ch 50-59", c, 11, c["mute"]))
    # core
    out.append(rect(20, 285, 760, 60, c, fill=c["grey_soft"], stroke=c["line"], r=10))
    out.append(text(400, 310, "Spark Core: RDDs, DAGScheduler, TaskScheduler, BlockManager, shuffle", c, 14, c["fg"], 700))
    out.append(text(400, 330, "ch 1, 70, 71", c, 11, c["mute"]))
    for i, (n, ch) in enumerate((("GraphX", "RDD graphs, Scala/Java"), ("DStreams", "deprecated since 3.4"))):
        x = 800 + i * 142
        out.append(rect(x, 285, 134, 60, c, fill=c["panel"], stroke=c["line"], r=8, dash="5 4"))
        out.append(text(x + 67, 310, n, c, 12, c["fg"], 600))
        out.append(text(x + 67, 330, ch, c, 10, c["mute"]))
    out.append(rect(20, 365, 1060, 50, c, fill=c["panel"], stroke=c["line"], r=10))
    out.append(text(550, 395, "cluster managers: local, standalone, YARN, Kubernetes (ch 47, 71)", c, 13, c["fg"], 600))
    return out


write("ch73-timeline", 1200, 510, timeline)
write("ch73-modules", 1100, 430, modules)
print("ok")

"""Figures for the SDP part (chapters 77 to 79), drawn as SVG by hand.

  python ch46/figures.py <out dir>      (the book uses book/media/figures)
Transparent background, mid-tone fills with white text, mid-grey lines and labels, so each
figure reads on the light and the dark theme. Every number comes from code/ch46..ch48/output.
"""
import sys
from pathlib import Path
from xml.sax.saxutils import escape

BLUE, ORANGE, GREEN, GREY, RED, PURPLE = "#3a5f96", "#c0661a", "#3f7d58", "#5b6472", "#b03a2e", "#6a4c93"
INK = "#8a8f98"   # labels and lines: mid grey, readable on white and on near-black
FONT = "font-family='Inter, DejaVu Sans, Helvetica, Arial, sans-serif'"
MONO = "font-family='JetBrains Mono, DejaVu Sans Mono, Menlo, monospace'"


class Svg:
    def __init__(self, w, h):
        self.w, self.h, self.items = w, h, []

    def box(self, x, y, w, h, fill, lines, size=15, mono=False, r=8, opacity=0.88, anchor="middle"):
        self.items.append(f"<rect x='{x}' y='{y}' width='{w}' height='{h}' rx='{r}' fill='{fill}' "
                          f"fill-opacity='{opacity}' stroke='{fill}'/>")
        n = len(lines)
        tx = x + w / 2 if anchor == "middle" else x + 12
        for i, line in enumerate(lines):
            ty = y + h / 2 + (i - (n - 1) / 2) * (size + 5) + size * 0.35
            weight = "700" if i == 0 else "400"
            self.items.append(f"<text x='{tx}' y='{ty:.1f}' {MONO if mono else FONT} font-size='{size if i == 0 else size - 2}' "
                              f"font-weight='{weight}' fill='#ffffff' text-anchor='{anchor}'>{escape(line)}</text>")

    def text(self, x, y, s, size=14, anchor="start", weight="400", color=INK, mono=False):
        self.items.append(f"<text x='{x}' y='{y}' {MONO if mono else FONT} font-size='{size}' font-weight='{weight}' "
                          f"fill='{color}' text-anchor='{anchor}'>{escape(s)}</text>")

    def arrow(self, x1, y1, x2, y2, label=None, dashed=False, color=INK, lx=None, ly=None):
        dash = " stroke-dasharray='6 5'" if dashed else ""
        self.items.append(f"<line x1='{x1}' y1='{y1}' x2='{x2}' y2='{y2}' stroke='{color}' stroke-width='2'{dash} "
                          f"marker-end='url(#arrow)'/>")
        if label:
            self.text(lx if lx is not None else (x1 + x2) / 2, ly if ly is not None else (y1 + y2) / 2 - 6,
                      label, size=12, anchor="middle")

    def frame(self, x, y, w, h, title):
        self.items.append(f"<rect x='{x}' y='{y}' width='{w}' height='{h}' rx='12' fill='none' stroke='{INK}' "
                          f"stroke-width='1.5' stroke-dasharray='4 4'/>")
        self.text(x + 14, y + 22, title, size=14, weight="700")

    def save(self, path):
        head = (f"<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 {self.w} {self.h}' width='{self.w}' height='{self.h}'>"
                f"<defs><marker id='arrow' viewBox='0 0 10 10' refX='9' refY='5' markerWidth='7' markerHeight='7' "
                f"orient='auto-start-reverse'><path d='M0,0 L10,5 L0,10 z' fill='{INK}'/></marker></defs>")
        Path(path).write_text(head + "\n".join(self.items) + "</svg>\n")


def project_layout(out):
    s = Svg(980, 560)
    s.text(20, 30, "taxi_etl/  (the project folder)", size=16, weight="700")
    files = [("spark-pipeline.yml", GREY, ["spark-pipeline.yml", "name, storage, libraries"]),
             ("raw.py", BLUE, ["transformations/raw.py", "@dp.table, @dp.materialized_view"]),
             ("clean_trips.sql", BLUE, ["transformations/clean_trips.sql", "CREATE STREAMING TABLE"]),
             ("borough_daily.sql", BLUE, ["transformations/borough_daily.sql", "CREATE MATERIALIZED VIEW"]),
             ("data", GREY, ["data/", "trips/*.parquet, taxi_zone_lookup.csv"])]
    ys = {}
    for i, (key, col, lines) in enumerate(files):
        y = 50 + i * 95
        s.box(20, y, 330, 66, col, lines, size=15, mono=False)
        ys[key] = y + 33
    s.text(520, 30, "what a run makes", size=16, weight="700")
    outs = [("raw_trips", BLUE, ["raw_trips", "streaming table"]),
            ("zones", ORANGE, ["zones", "materialized view"]),
            ("clean_trips", BLUE, ["clean_trips", "streaming table"]),
            ("borough_daily", ORANGE, ["borough_daily", "materialized view"])]
    oy = {}
    for i, (key, col, lines) in enumerate(outs):
        y = 50 + i * 82
        s.box(450, y, 200, 58, col, lines, size=15)
        oy[key] = y + 29
    s.arrow(350, ys["raw.py"], 448, oy["raw_trips"])
    s.arrow(350, ys["raw.py"], 448, oy["zones"])
    s.arrow(350, ys["clean_trips.sql"], 448, oy["clean_trips"])
    s.arrow(350, ys["borough_daily.sql"], 448, oy["borough_daily"])
    disk = [(GREEN, ["spark-warehouse/<table>/", "Parquet files of each table"], 60),
            (GREEN, ["metastore_db/", "Hive catalog: names, schemas"], 170),
            (PURPLE, ["pipeline-storage/_checkpoints/", "spark_catalog/default/<table>/<flow>/0", "(the storage key of the spec)"], 280)]
    s.text(720, 30, "files on disk", size=16, weight="700")
    for col, lines, y in disk:
        s.box(700, y, 265, 80 if len(lines) > 2 else 70, col, lines, size=14)
    for key in ["raw_trips", "zones", "clean_trips", "borough_daily"]:
        s.arrow(650, oy[key], 698, 95)
    s.arrow(650, oy["raw_trips"], 698, 205, dashed=True)
    s.arrow(650, oy["raw_trips"], 698, 315, dashed=True)
    s.arrow(650, oy["clean_trips"], 698, 315, dashed=True)
    s.text(450, 400, "Only the streaming tables get a checkpoint.", size=13)
    s.text(450, 420, "The materialized views read all of their input in each run.", size=13)
    s.text(450, 460, "spark-warehouse/ and metastore_db/ are in the current folder of", size=13)
    s.text(450, 480, "the Connect server (the project folder in Chapter 46).", size=13)
    s.save(out / "ch46-project-layout.svg")


def two_runs(out):
    s = Svg(980, 470)
    runs = [("run 1: day 2026-06-01", {"raw": (111546, 111546), "clean": (104952, 104952), "agg": (7, 7)}),
            ("run 2: day 2026-06-02 added", {"raw": (126260, 237806), "clean": (118492, 223444), "agg": (14, 14)})]
    scale = 300 / 237806
    for r, (title, d) in enumerate(runs):
        x0 = 30 + r * 480
        s.text(x0, 34, title, size=16, weight="700")
        rows = [("raw_trips", "streaming table: reads only new files", BLUE, d["raw"]),
                ("clean_trips", "streaming table: reads only new rows", BLUE, d["clean"]),
                ("borough_daily", "materialized view: computed from all rows", ORANGE, d["agg"])]
        for i, (name, sub, col, (read, total)) in enumerate(rows):
            y = 60 + i * 120
            s.text(x0, y + 14, name, size=15, weight="700", mono=True)
            s.text(x0, y + 32, sub, size=12)
            if name == "borough_daily":
                s.box(x0, y + 44, 300, 40, col, [f"{read} rows written (all)"], size=14)
                s.text(x0 + 310, y + 70, f"table: {total} rows", size=13)
            else:
                old = total - read
                if old:
                    s.items.append(f"<rect x='{x0}' y='{y + 44}' width='{old * scale:.1f}' height='40' rx='6' "
                                   f"fill='{GREY}' fill-opacity='0.35' stroke='{GREY}'/>")
                    s.text(x0 + old * scale / 2, y + 69, f"{old:,} kept", size=12, anchor="middle")
                s.box(x0 + old * scale, y + 44, read * scale, 40, col, [f"+{read:,}"], size=14)
                s.text(x0 + 310, y + 70, f"table: {total:,}", size=13)
    s.text(30, 440, "Grey: rows from the earlier run, not read again. Colour: rows that the run read and wrote.", size=13)
    s.save(out / "ch46-two-runs.svg")


def argument_split(out):
    s = Svg(980, 430)
    s.box(20, 30, 940, 56, GREY, ["spark-pipelines --remote sc://localhost:15104 --conf spark.sql.shuffle.partitions=8 run --refresh zones"],
          size=15, mono=True)
    s.box(330, 130, 320, 60, PURPLE, ["SparkPipelines.splitArgs", "core/.../deploy/SparkPipelines.scala"], size=15)
    s.arrow(490, 86, 490, 128)
    s.box(20, 250, 420, 120, BLUE, ["spark-submit", "--remote sc://localhost:15104", "--conf spark.sql.shuffle.partitions=8",
                                     "+ --conf spark.api.mode=connect (always)"], size=15, mono=True)
    s.box(540, 250, 420, 120, ORANGE, ["cli.py (PythonRunner)", "run --refresh zones",
                                        "also: --spec, --full-refresh, --name, -h"], size=15, mono=True)
    s.arrow(400, 190, 230, 248, label="options that spark-submit knows", lx=190, ly=222)
    s.arrow(580, 190, 750, 248, label="unknown options and words", lx=800, ly=222)
    s.box(400, 385, 180, 36, RED, ["--class: refused"], size=14)
    s.text(20, 412, "No --remote: the wrapper adds --remote local", size=13)
    s.save(out / "ch47-argument-split.svg")


def client_server(out):
    s = Svg(980, 470)
    s.frame(20, 20, 400, 430, "client: spark-pipelines (cli.py)")
    s.frame(560, 20, 400, 430, "Spark Connect server")
    s.box(45, 60, 350, 52, GREY, ["spark-pipeline.yml", "found from --spec or the current folder"], size=14)
    s.box(45, 130, 350, 52, BLUE, ["transformations/*.py, *.sql", "globs relative to the spec folder"], size=14)
    s.box(45, 200, 350, 52, BLUE, ["module code, query functions", "run here, at registration"], size=14)
    s.box(45, 270, 350, 52, GREY, ["environment variables", "visible to module code only"], size=14)
    s.box(585, 60, 350, 52, ORANGE, ["graph: resolve, check, run", "CreateDataflowGraph, StartRun"], size=14)
    s.box(585, 130, 350, 52, GREEN, ["data paths in queries", "relative to the server folder"], size=14)
    s.box(585, 200, 350, 52, GREEN, ["spark-warehouse/, metastore_db/", "server folder, or the catalog"], size=14)
    s.box(585, 270, 350, 52, PURPLE, ["storage: _checkpoints/...", "absolute URI from the spec"], size=14)
    s.box(585, 340, 350, 52, GREY, ["jars, static configurations", "given when the server starts"], size=14)
    s.arrow(420, 150, 583, 86, label="plans, SQL text", lx=500, ly=104)
    s.arrow(583, 110, 420, 230, label="events", lx=500, ly=190)
    s.text(45, 380, "PATH_NOT_FOUND: the server could not find a", size=13)
    s.text(45, 400, "relative path in its own current folder.", size=13)
    s.save(out / "ch47-client-server.svg")


def two_environments(out):
    s = Svg(980, 440)
    s.box(330, 20, 320, 60, BLUE, ["transformations/", "raw.py, clean_trips.sql, borough_daily.sql"], size=14)
    s.box(100, 130, 300, 90, GREY, ["envs/dev.yml", "database: taxi_dev", "shuffle 4, taxi.min_fare 0"], size=14, mono=True)
    s.box(580, 130, 300, 90, GREY, ["envs/prod.yml", "database: taxi_prod", "shuffle 16, taxi.min_fare 2.50"], size=14, mono=True)
    s.arrow(430, 80, 260, 128, label="../transformations/**", lx=250, ly=100)
    s.arrow(550, 80, 720, 128, label="../transformations/**", lx=740, ly=100)
    s.box(100, 270, 300, 60, GREEN, ["taxi_dev.clean_trips", "223,444 rows"], size=15)
    s.box(580, 270, 300, 60, GREEN, ["taxi_prod.clean_trips", "223,408 rows"], size=15)
    s.box(100, 350, 300, 50, PURPLE, ["storage/dev/_checkpoints/", "spark_catalog/taxi_dev"], size=13)
    s.box(580, 350, 300, 50, PURPLE, ["storage/prod/_checkpoints/", "spark_catalog/taxi_prod"], size=13)
    s.arrow(250, 220, 250, 268)
    s.arrow(730, 220, 730, 268)
    s.text(490, 300, "same code", size=13, anchor="middle")
    s.text(490, 320, "different spec", size=13, anchor="middle")
    s.save(out / "ch48-two-environments.svg")


def config_layers(out):
    s = Svg(980, 520)
    layers = [(GREY, "1  server start", "--jars, static: catalogImplementation, extensions",
               "in the spec: each flow failed, CANNOT_MODIFY_STATIC_CONFIG"),
              (BLUE, "2  CLI --conf", "spark.sql.* keys only reach a remote session",
               "shuffle.partitions=5 arrived, taxi.min_fare=9 did not"),
              (ORANGE, "3  spec configuration", "the session, and each flow (defaultSqlConf)",
               "replaced the CLI value: 3 shuffle partitions, 3 files"),
              (PURPLE, "4  decorator spark_conf", "merged after the spec for one flow",
               "shuffle.partitions=2 did not reach the write: still 3 files")]
    for i, (col, title, sub, result) in enumerate(layers):
        y = 30 + i * 118
        s.box(30, y, 420, 84, col, [title, sub], size=16, anchor="start")
        s.text(480, y + 36, "tested:", size=13, weight="700")
        s.text(480, y + 58, result, size=13)
        if i < 3:
            s.arrow(240, y + 84, 240, y + 116)
    s.text(30, 505, "A lower layer replaces a value from a higher layer, except where the test says otherwise.", size=13)
    s.save(out / "ch48-config-layers.svg")


if __name__ == "__main__":
    out = Path(sys.argv[1])
    out.mkdir(parents=True, exist_ok=True)
    for f in (project_layout, two_runs, argument_split, client_server, two_environments, config_layers):
        f(out)
        print("wrote", f.__name__)

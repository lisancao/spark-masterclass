"""Chapter 81 figures: what each security setting protects, and what an eavesdropper read in auth.sh
and io_encryption.py. Reads code/ch81/output/*.txt.

Run from code/:  FIG_CHECK_DIR=<scratch> uv run python ch81/figures.py
"""
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.svgfig import Fig  # noqa: E402

OUT = Path(__file__).resolve().parent / "output"


def surfaces():
    f = Fig(940, 470, "What each setting protects")
    f.text(20, 28, "The surfaces of a Spark application, and the setting that protects each one", 17, 700, anchor="start")
    f.box(330, 60, 280, 80, "driver", "SparkContext, UI on 4040", kind="acc", size=15)
    f.box(40, 250, 220, 70, "executor", kind="n", size=15)
    f.box(360, 250, 220, 70, "executor", kind="n", size=15)
    f.box(690, 250, 220, 70, "master / worker", "(standalone)", kind="n", size=15)
    f.box(690, 60, 220, 80, "browser, curl", "UI and REST API", kind="ghost", size=15)
    f.box(40, 60, 220, 80, "Connect client", "gRPC, port 15002", kind="ghost", size=15)
    f.box(40, 380, 320, 60, "local disk: shuffle, spill, cache", kind="purple", size=13)
    f.box(400, 380, 240, 60, "event log, UI Environment", kind="purple", size=13)
    f.box(680, 380, 230, 60, "Kerberos services", "HDFS, Hive metastore", kind="purple", size=13, subsize=11)
    f.arrow(380, 140, 160, 250, "", both=True)
    f.arrow(470, 140, 470, 250, "", both=True)
    f.arrow(600, 140, 800, 250, "", both=True)
    f.arrow(260, 285, 360, 285, "", both=True)
    f.text(482, 190, "RPC: spark.authenticate", 12, 600, anchor="start", c="blue")
    f.text(482, 206, "+ spark.network.crypto.enabled", 12, 400, anchor="start", c="blue")
    f.text(482, 222, "or spark.ssl.rpc.enabled", 12, 400, anchor="start", c="blue")
    f.text(310, 278, "shuffle fetch", 11, 400, c="mute")
    f.arrow(690, 100, 610, 100, "", both=True)
    f.text(700, 162, "spark.ssl.ui.*, spark.ui.filters,", 12, 600, anchor="start", c="ok")
    f.text(700, 178, "spark.acls.enable", 12, 400, anchor="start", c="ok")
    f.arrow(260, 100, 330, 100, "", both=True)
    f.text(40, 162, "spark.connect.authenticate.token", 12, 600, anchor="start", c="ok")
    f.text(40, 178, "+ TLS at a proxy (Chapter 24)", 12, 400, anchor="start", c="ok")
    f.text(200, 365, "spark.io.encryption.enabled", 12, 600, c="purple")
    f.text(520, 365, "spark.redaction.regex", 12, 600, c="purple")
    f.text(795, 365, "spark.kerberos.*, tokens", 12, 600, c="purple")
    f.note("schematic; the ports are the defaults")
    f.save("ch81-surfaces")


def eavesdrop():
    t = (OUT / "auth.txt").read_text()
    runs = re.findall(r"^(no-secret|wrong-secret|secret-no-crypto|secret-aes-gcm)\s+(.*)\n(?:.*master log:.*\n)?\s+(\d+) bytes to the master, app name visible: (\d+), \"spark.\" strings: (\d+)", t, re.M)
    io = (OUT / "io_encryption.txt").read_text()
    heads = re.findall(r"(shuffle data|cached block)\s+(\S+)\s+([\d,]+) B\s+((?:[0-9a-f]{8} ?){6})\s+\|(.*)\|", io)
    f = Fig(940, 470, "What an eavesdropper reads")
    f.text(20, 28, "What a listener on the wire and a reader of the disk see (auth.txt, io_encryption.txt)", 17, 700, anchor="start")
    rows = [["run (driver to master, through a proxy)", "result", "bytes", "app name", "\"spark.\" strings"]]
    for name, res, b, app, sp in runs:
        rows.append([name, "ok" if "ok," in res else "refused", b, "visible" if app != "0" else "-", sp])
    hi = [i for i, r in enumerate(rows) if r[0] == "secret-no-crypto"]
    ok = [i for i, r in enumerate(rows) if r[0] == "secret-aes-gcm"]
    f.table(20, 50, [300, 110, 90, 110, 140], rows, rh=30, size=12.5, mono_cols=(0, 2, 4), bad_rows=tuple(hi),
            ok_rows=tuple(ok), align=["start", "start", "end", "start", "end"])
    f.text(20, 230, "First 24 bytes of a file in spark.local.dir", 14, 700, anchor="start")
    y = 245
    for i, (kind, name, size, hx, txt) in enumerate(heads):
        on = i >= 2
        f.rect(20, y, 900, 46, "ok" if on else "bad", r=6)
        f.text(32, y + 19, f"{'encryption on ' if on else 'encryption off'}  {kind}  {name}  ({size} B)", 12, 600,
               anchor="start", c="ok" if on else "bad")
        f.text(32, y + 37, f"{hx.strip()}   |{txt}|", 12, 400, anchor="start", mono=True, c="fg")
        y += 52
    f.save("ch81-eavesdrop")


surfaces()
eavesdrop()

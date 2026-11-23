#!/usr/bin/env python3
"""Screenshots of every Spark UI tab for the Chapter 33 reference: headless Chromium at 1280x800.

Starts ch33/ui_tabs.py, waits until it is ready, takes one screenshot per tab, then lets it stop.
PNGs go to book/media/figures/ch33-ui-<tab>.png. Run from code/ with the system Python (needs playwright):
  python3 film/ch33_ui_tabs.py
"""
import os, re, subprocess, sys, time
from pathlib import Path
from playwright.sync_api import sync_playwright

CODE = Path(__file__).resolve().parents[1]
FIG = CODE.parent / "book" / "media" / "figures"
PORT = os.environ.get("SPARK_UI_PORT", "4040")
UI = f"http://localhost:{PORT}"
TMP = Path("/tmp/masterclass-ch33")
READY, DONE = TMP / "tabs_ready", TMP / "tabs_done"
ORANGE = "#E25A1C"
HOME = str(Path.home())
TMP.mkdir(parents=True, exist_ok=True)
for f in (READY, DONE):
    f.unlink(missing_ok=True)
env = dict(os.environ, UI_TABS_READY=str(READY), UI_TABS_DONE=str(DONE))
tour = subprocess.Popen(["uv", "run", "python", "ch33/ui_tabs.py"], cwd=CODE, env=env,
                        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
while not READY.exists():
    if tour.poll() is not None:
        sys.exit("ui_tabs.py stopped before it was ready")
    time.sleep(1)


def shot(page, path, name, marks=(), scroll_to=None, wait=2.5, click=None):
    page.goto(f"{UI}{path}")
    time.sleep(wait)
    if click:
        page.get_by_text(click, exact=False).filter(visible=True).first.click()
        time.sleep(1.5)
    page.evaluate("""h => { const w = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
        while (w.nextNode()) { const n = w.currentNode; if (n.nodeValue.includes(h)) n.nodeValue = n.nodeValue.split(h).join('~'); } }""", HOME)
    for m in marks:
        try:
            loc = page.get_by_text(m, exact=False).filter(visible=True).first
            loc.evaluate(f"e => {{ e.style.outline = '3px solid {ORANGE}'; e.style.outlineOffset = '2px'; }}")
        except Exception as e:
            print(f"  no mark {m!r} on {name}: {e.__class__.__name__}")
    if scroll_to:
        try:
            page.get_by_text(scroll_to, exact=False).filter(visible=True).first.scroll_into_view_if_needed()
            page.evaluate("window.scrollBy(0, -80)")
        except Exception:
            pass
    out = FIG / f"ch33-ui-{name}.png"
    page.screenshot(path=str(out))
    small(out)
    print(f"{out.name}: {out.stat().st_size // 1024} KB")


def small(path):
    """Quantize a screenshot to 128 colours with pngquant, to keep the book's media small."""
    subprocess.run(["pngquant", "--force", "--skip-if-larger", "--quality=70-95", "128", "--output", str(path), str(path)])


try:
    with sync_playwright() as p:
        b = p.chromium.launch()
        page = b.new_context(viewport={"width": 1280, "height": 800}, device_scale_factor=1).new_page()
        shot(page, "/jobs/", "jobs", marks=["1. trips by pickup zone", "Event Timeline"])
        page.goto(f"{UI}/stages/"); time.sleep(2)
        sid = max(int(x) for x in re.findall(r"stage/\?id=(\d+)&amp;attempt=0", page.content()) if x)
        stages = subprocess.run(["curl", "-s", f"{UI}/api/v1/applications"], capture_output=True, text=True).stdout
        shot(page, "/stages/", "stages", marks=["Completed Stages"])
        # the stage of the spilling sort: the stage with the most memory spill
        import json, urllib.request
        app = json.load(urllib.request.urlopen(f"{UI}/api/v1/applications"))[0]["id"]
        st = json.load(urllib.request.urlopen(f"{UI}/api/v1/applications/{app}/stages?status=complete"))
        spill = max(st, key=lambda s: s["memoryBytesSpilled"])["stageId"]
        shot(page, f"/stages/stage/?id={spill}&attempt=0", "stage-detail", marks=["Summary Metrics for", "Spill (Memory)"],
             scroll_to="Summary Metrics for")
        shot(page, "/storage/", "storage", marks=["Memory Deserialized 1x Replicated"])
        shot(page, "/environment/", "environment", marks=[], click="Spark Properties")
        shot(page, "/executors/", "executors", marks=["Summary", "GC Time"])
        sql = json.load(urllib.request.urlopen(f"{UI}/api/v1/applications/{app}/sql?details=false&length=1000"))
        first = [e for e in sql if (e.get("description") or "").startswith("1.")][-1]["id"]
        shot(page, "/SQL/", "sql", marks=["Completed Queries", "Completed"])
        shot(page, f"/SQL/execution/?id={first}", "sql-detail", marks=["Exchange", "HashAggregate"], wait=4)
        shot(page, "/StreamingQuery/", "streaming", marks=["rate_windows"])
        page.goto(f"{UI}/StreamingQuery/"); time.sleep(2)
        link = page.locator("a[href*='StreamingQuery/statistics']").first
        link.click(); time.sleep(4)
        page.screenshot(path=str(FIG / "ch33-ui-streaming-detail.png")); small(FIG / "ch33-ui-streaming-detail.png")
        print("ch33-ui-streaming-detail.png")
        shot(page, "/connect/", "connect", marks=["reader", "ui-tour"])
        b.close()
finally:
    DONE.write_text("done")
    out, _ = tour.communicate(timeout=120)
    (CODE / "ch33" / "output" / "ui_tabs.txt").write_text(out.replace(HOME, "~"))

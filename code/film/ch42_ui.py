#!/usr/bin/env python3
"""Record the Chapter 42 Spark UI walkthrough: headless Chromium at 1280x720, driven by Playwright.

It starts ch42/ui_tour.py, waits until the workload is done, records two clips of the live UI,
then lets the application stop. Clips go to book/media/clips/ as .mp4 with a .webp poster.
Run from code/ with the system Python (it needs playwright):
  flock ~/spark-masterclass/data/.bench.lock python3 film/ch42_ui.py
"""
import os, re, shutil, subprocess, sys, time
from pathlib import Path
from playwright.sync_api import sync_playwright

CODE = Path(__file__).resolve().parents[1]
CLIPS = CODE.parent / "book" / "media" / "clips"
PORT = os.environ.get("SPARK_UI_PORT", "4024")
UI = f"http://localhost:{PORT}"
TMP = Path("/tmp/masterclass-ch42")
READY, DONE = TMP / "ui_ready", TMP / "ui_done"
ORANGE = "#E25A1C"

TMP.mkdir(parents=True, exist_ok=True)
for f in (READY, DONE):
    f.unlink(missing_ok=True)
env = dict(os.environ, SPARK_UI_PORT=PORT, UI_TOUR_READY=str(READY), UI_TOUR_DONE=str(DONE),
           BOOK_DATA=os.environ.get("BOOK_DATA", str(Path.home() / "spark-masterclass/data")))
tour = subprocess.Popen(["uv", "run", "python", "ch42/ui_tour.py"], cwd=CODE, env=env,
                        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
while not READY.exists():
    if tour.poll() is not None:
        sys.exit("ui_tour.py stopped before it was ready")
    time.sleep(1)
spill_stage, sql_id = READY.read_text().split()


def mark(page, text, exact=True):
    """Draw an orange box around the first element with this text."""
    loc = page.get_by_text(text, exact=exact).filter(visible=True).first
    loc.scroll_into_view_if_needed()
    loc.evaluate(f"e => {{ e.style.outline = '3px solid {ORANGE}'; e.style.outlineOffset = '2px'; }}")
    return loc


HOME = str(Path.home())


def go(page, url, wait):
    """Open a page, and show the home folder as ~ in the call sites that the UI prints."""
    page.goto(url)
    page.evaluate("""h => { const w = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
        while (w.nextNode()) { const n = w.currentNode; if (n.nodeValue.includes(h)) n.nodeValue = n.nodeValue.split(h).join('~'); } }""", HOME)
    time.sleep(wait)


def glide(page, y_to, steps=30):
    y0 = page.evaluate("window.scrollY")
    for i in range(1, steps + 1):
        page.evaluate(f"window.scrollTo(0, {y0 + (y_to - y0) * i / steps})")
        time.sleep(0.03)


def clip_jobs_stages(page):
    go(page, f"{UI}/jobs/", 4)
    mark(page, "1. sort six months by distance, 2 partitions, AQE off", exact=False); time.sleep(3)
    go(page, f"{UI}/stages/stage/?id={spill_stage}&attempt=0", 3)
    mark(page, "Summary Metrics for", exact=False); time.sleep(1.5)
    mem, disk = re.compile(r"^\s*Spill \(memory\)", re.I), re.compile(r"^\s*Spill \(disk\)", re.I)
    page.get_by_text(mem).filter(visible=True).first.scroll_into_view_if_needed()
    glide(page, max(0, page.evaluate("window.scrollY") - 160))
    mark(page, mem); mark(page, disk); time.sleep(5)


def clip_storage_sql(page):
    go(page, f"{UI}/storage/", 3)
    mark(page, "Memory Deserialized 1x Replicated", exact=False); time.sleep(4)
    go(page, f"{UI}/SQL/execution/?id={sql_id}", 4)
    for name in ("InMemoryTableScan", "Exchange"):
        try:
            mark(page, name, exact=False); time.sleep(2.5)
        except Exception:
            pass
    glide(page, 600); time.sleep(3)


def record(name, drive):
    vids = TMP / f"video-{name}"
    shutil.rmtree(vids, ignore_errors=True)
    with sync_playwright() as p:
        browser = p.chromium.launch()
        ctx = browser.new_context(viewport={"width": 1280, "height": 720}, device_scale_factor=1,
                                  record_video_dir=str(vids), record_video_size={"width": 1280, "height": 720})
        page = ctx.new_page()
        page.goto(f"{UI}/"); time.sleep(1)
        drive(page)
        ctx.close(); browser.close()
    webm = next(vids.glob("*.webm"))
    out = CLIPS / f"{name}.mp4"
    subprocess.run(["ffmpeg", "-y", "-v", "error", "-ss", "1", "-i", str(webm), "-c:v", "libx264", "-preset", "slow",
                    "-crf", "23", "-pix_fmt", "yuv420p", "-movflags", "+faststart", "-an", str(out)], check=True)
    dur = float(subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", str(out)],
                               capture_output=True, text=True).stdout)
    subprocess.run(["ffmpeg", "-y", "-v", "error", "-ss", f"{dur - 1.5:.1f}", "-i", str(out), "-frames:v", "1",
                    "-c:v", "libwebp", "-quality", "80", str(CLIPS / f"{name}.webp")], check=True)
    print(f"{out.name}: {dur:.1f} s")


try:
    record("ch42-ui-stages", clip_jobs_stages)
    record("ch42-ui-storage-sql", clip_storage_sql)
finally:
    DONE.write_text("done")
    out, _ = tour.communicate(timeout=120)
    (CODE / "ch42" / "output" / "ui_tour.txt").write_text(out.replace(str(Path.home()), "~"))

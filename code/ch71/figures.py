"""Chapter 71 figures: the driver and executor parts, the shuffle on disk, and memory eviction.

Reads code/ch71/output/anatomy.txt and memory.txt. Run from code/:  uv run python ch71/figures.py
"""
import re
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.svgfig_bg import write, text, box, arrow, rect

OUT = Path(__file__).resolve().parent / "output"
anat = (OUT / "anatomy.txt").read_text()
mem = (OUT / "memory.txt").read_text()

# map outputs: name -> (executor folder, partition sizes)
sizes = {m[1]: [int(x) for x in m[2].split(", ")]
         for m in re.finditer(r"(shuffle_0_\d_0)\.index\s+\[([\d, ]+)\]", anat)}
where = {}
for m in re.finditer(r"(executor-\w+)\.\.\./blockmgr-\w+\.\.\./\w+/(shuffle_0_\d_0)\.data", anat):
    where[m[2]] = m[1]
reduce = [(int(m[1]), m[2], int(m[3].replace(",", "")), int(m[4].replace(",", "")))
          for m in re.finditer(r"task\s+(\d)\s+executor (\d)\s+NODE_LOCAL\s+\d+ ms\s+fetched local ([\d,]+) B remote ([\d,]+) B", anat)]
map_exec = {}
for m in re.finditer(r"task\s+(\d)\s+executor (\d)\s+PROCESS_LOCAL\s+\d+ ms\s+rows in\s+[\d,]+\s+shuffle write\s+([\d,]+) B", anat):
    map_exec[int(m[3].replace(",", ""))] = m[2]


def shuffle(c):
    out = [text(20, 26, "One shuffle on disk: 4 map outputs x 6 reduce partitions (anatomy.txt)", c, 16, c["fg"], 700, "start")]
    names = sorted(sizes)
    colors = [c["acc"], c["blue"], c["green"], c["purple"], c["red"], c["mute"]]
    x0, y0, scale = 230, 60, 0.13
    for i, n in enumerate(names):
        y = y0 + i * 62
        total = sum(sizes[n])
        ex = map_exec.get(total, "?")
        out.append(text(20, y + 22, f"{n}.data", c, 13, c["fg"], 600, "start", mono=True))
        out.append(text(20, y + 40, f"executor {ex}, {total:,} B", c, 12, c["mute"], 400, "start"))
        x = x0
        for p, s in enumerate(sizes[n]):
            w = s * 750 / 5100
            out.append(rect(x, y + 6, w, 34, c, fill=colors[p], stroke=c["bg"], r=3, sw=1))
            out.append(text(x + w / 2, y + 28, f"{s}", c, 11, "#ffffff", 600))
            x += w
    # index offsets for the first map output
    o = [0]
    for s in sizes[names[0]]:
        o.append(o[-1] + s)
    yi = y0 + 4 * 62 + 10
    out.append(text(20, yi + 18, f"{names[0]}.index", c, 13, c["fg"], 600, "start", mono=True))
    out.append(text(20, yi + 36, "7 big-endian longs", c, 12, c["mute"], 400, "start"))
    out.append(text(x0, yi + 26, "offsets: " + ", ".join(str(v) for v in o), c, 13, c["fg"], 400, "start", mono=True))
    yr = yi + 70
    out.append(text(20, yr, "reduce tasks (stage 2) fetch one segment from every map output:", c, 13, c["fg"], 600, "start"))
    for k, (idx, ex, loc, rem) in enumerate(reduce):
        x = 20 + k * 165
        out.append(rect(x, yr + 12, 150, 58, c, fill=c["panel"], stroke=colors[idx], r=6, sw=2))
        out.append(text(x + 75, yr + 31, f"partition {idx}, executor {ex}", c, 12, c["fg"], 600))
        out.append(text(x + 75, yr + 48, f"local {loc:,} B", c, 11, c["mute"]))
        out.append(text(x + 75, yr + 63, f"remote {rem:,} B", c, 11, c["mute"]))
    return out


def parts(c):
    out = [text(20, 26, "The parts of an application, with their class names in Spark 4.3", c, 16, c["fg"], 700, "start")]
    # driver
    out.append(rect(20, 45, 420, 330, c, fill=c["panel"], stroke=c["blue"], r=12, sw=2))
    out.append(text(230, 70, "driver JVM", c, 15, c["blue"], 700))
    items = [("SparkContext", "creates the parts below"),
             ("DAGScheduler", "job -> stages -> TaskSets"),
             ("TaskSchedulerImpl", "TaskSetManager, locality"),
             ("CoarseGrainedSchedulerBackend", "offers, launches, RPC"),
             ("BlockManagerMaster", "where every block is"),
             ("MapOutputTracker", "where every map output is"),
             ("LiveListenerBus", "UI, event log, listeners")]
    for i, (n, s) in enumerate(items):
        y = 85 + i * 40
        out.append(rect(40, y, 380, 34, c, fill=c["blue_soft"], stroke=c["blue"], r=6))
        out.append(text(52, y + 22, n, c, 13, c["fg"], 600, "start", mono=True))
        out.append(text(408, y + 22, s, c, 12, c["mute"], 400, "end"))
    # executors
    for k in range(2):
        x = 520 + k * 300
        out.append(rect(x, 45, 280, 330, c, fill=c["panel"], stroke=c["acc"], r=12, sw=2))
        out.append(text(x + 140, 70, f"executor {k} JVM", c, 15, c["acc"], 700))
        ex = [("CoarseGrainedExecutorBackend", 34), ("Executor: task threads", 50),
              ("BlockManager", 34), ("UnifiedMemoryManager", 34), ("SortShuffleManager", 34)]
        y = 85
        for n, h in ex:
            out.append(rect(x + 15, y, 250, h, c, fill=c["acc_soft"], stroke=c["acc"], r=6))
            out.append(text(x + 140, y + (19 if h > 40 else h / 2 + 5), n, c, 12, c["fg"], 600, mono=True))
            if n.startswith("Executor"):
                for t in range(2):
                    out.append(rect(x + 70 + t * 75, y + 28, 60, 14, c, fill=c["acc"], stroke=c["acc"], r=3))
            y += h + 8
        out.append(text(x + 140, y + 30, "local dir: blockmgr-*/", c, 12, c["mute"], mono=True))
        out.append(text(x + 140, y + 48, "shuffle files, spills", c, 12, c["mute"]))
    out.append(arrow(440, 230, 518, 230, c, color=c["blue"]))
    out.append(text(479, 222, "tasks", c, 11, c["mute"]))
    out.append(arrow(518, 260, 440, 260, c, color=c["acc"]))
    out.append(text(479, 278, "status", c, 11, c["mute"]))
    out.append(arrow(800, 300, 820, 300, c, color=c["green"]))
    out.append(arrow(820, 310, 800, 310, c, color=c["green"]))
    out.append(text(810, 395, "shuffle fetch between executors", c, 12, c["green"], 600))
    return out


def eviction(c):
    pool = float(re.search(r"maxMemory\): ([\d.]+) MiB", mem)[1])
    floor = float(re.search(r"protects ([\d.]+) MiB", mem)[1])
    before = float(re.search(r"after the cache:\s+(\d+)/32 partitions cached, ([\d.]+) MiB", mem)[2])
    m_after = re.search(r"after the sort:\s+(\d+)/32 partitions cached, ([\d.]+) MiB", mem)
    after, parts_after = float(m_after[2]), int(m_after[1])
    W, L = 760, 150
    sc_ = (W - L - 40) / pool
    out = [text(20, 26, f"The unified pool of a 1 GiB driver: {pool} MiB (memory.txt)", c, 16, c["fg"], 700, "start")]
    for i, (label, used, n) in enumerate((("after cache()", before, 32), ("after the sort", after, parts_after))):
        y = 60 + i * 80
        out.append(text(20, y + 26, label, c, 13, c["fg"], 600, "start"))
        out.append(rect(L, y, pool * sc_, 40, c, fill=c["panel"], stroke=c["line"], r=4))
        out.append(rect(L, y, used * sc_, 40, c, fill=c["blue"], stroke=c["blue"], r=4))
        out.append(text(L + used * sc_ / 2, y + 25, f"storage {used} MiB, {n}/32 partitions", c, 12, "#ffffff", 600))
    xf = L + floor * sc_
    out.append(f'<line x1="{xf}" y1="50" x2="{xf}" y2="200" stroke="{c["acc"]}" stroke-width="2" stroke-dasharray="5 4"/>')
    out.append(text(xf + 6, 215, f"storageFraction 0.5 = {floor} MiB: execution cannot evict below this line", c, 12, c["acc"], 600, "start"))
    out.append(text(L, 240, "The sort needed execution memory. It evicted cached blocks until storage was under the line.", c, 12, c["mute"], 400, "start"))
    return out


write("ch71-shuffle-files", 1000, 470, shuffle)
write("ch71-parts", 1120, 420, parts)
write("ch71-eviction", 860, 255, eviction)
print("ok", sorted(sizes), reduce[:1], map_exec)

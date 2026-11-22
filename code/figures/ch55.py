"""Figures for Chapter 55. Plan nodes from code/ch55/output/capability.txt and jdocs.txt."""
from svgkit import *

def tree(s, x, y, nodes, w=220):
    """nodes: list of (label, kind) from the top of the plan to the scan."""
    for i, (lab, kind) in enumerate(nodes):
        c = {"spark": GRAY, "py": ORANGE, "j": BLUE, "x": RED, "local": GREEN}[kind]
        s.box(x, y + i * 42, w, 30, lab, c, 11, mono=True)
        if i:
            s.line(x + w / 2, y + i * 42 - 1, x + w / 2, y + i * 42 - 11, MUTE, 1.5, arrow=False)
    return y + len(nodes) * 42

# 1. The same three questions, two sources (capability.txt)
s = Svg(780, 420, "Physical plans of three queries on pydocs and on jdocs")
qs = ["filter + two columns", "limit(3)", "count(*)"]
for k, q in enumerate(qs):
    s.text(180 + k * 238, 22, q, 13, ACC, "middle", 700)
s.text(8, 130, "pydocs", 13, ORANGE, "start", 700)
s.text(8, 345, "jdocs", 13, BLUE, "start", 700)
py = [[("Project [name, width]", "spark"), ("Filter isnotnull(kind)", "spark"), ("BatchScan pydocs", "py"), ("7 columns, EqualTo(kind,image)", "py")],
      [("CollectLimit 3", "spark"), ("Project [name]", "spark"), ("BatchScan pydocs", "py"), ("7 columns, 26 files", "py")],
      [("HashAggregate count(1)", "spark"), ("Exchange SinglePartition", "x"), ("HashAggregate partial_count", "spark"), ("BatchScan pydocs", "py")]]
jv = [[("Project [name, width]", "spark"), ("BatchScan jdocs", "j"), ("files=5, pushed=[kind='image']", "j")],
      [("Project [name]", "spark"), ("BatchScan jdocs", "j"), ("files=3, limit=3", "j")],
      [("Project [count]", "spark"), ("LocalTableScan [count]", "local")]]
for k in range(3):
    tree(s, 70 + k * 238, 40, py[k])
    tree(s, 70 + k * 238, 278, jv[k])
s.line(10, 245, 770, 245, MUTE, 1, dash=True, arrow=False)
s.text(390, 410, "Red: a shuffle. Green: rows that the source calculated on the driver, with no scan.", 11, MUTE, "middle", italic=True)
s.save(FIG + "ch55-three-plans.svg")

# 2. Reported partitioning removes the shuffle (jdocs.txt)
s = Svg(780, 330, "Final plans of groupBy(kind).agg(sum(size)) with and without KeyGroupedPartitioning")
s.text(140, 22, "byKind=false", 14, MUTE, "middle", 700)
s.text(560, 22, "byKind=true: KeyGroupedPartitioning(kind)", 14, ACC, "middle", 700)
y = tree(s, 20, 40, [("HashAggregate sum(size)", "spark"), ("AQEShuffleRead coalesced", "spark"), ("Exchange hashpartitioning(kind, 200)", "x"),
                     ("HashAggregate partial_sum(size)", "spark"), ("Project [kind, size]", "spark"), ("BatchScan jdocs files=26", "j")], w=250)
s.text(145, y + 10, "two stages", 12, MUTE)
y2 = tree(s, 440, 40, [("HashAggregate sum(size)", "spark"), ("Project [kind, size]", "spark"), ("BatchScan jdocs files=26", "j")], w=250)
s.text(565, y2 + 10, "one stage: no Exchange, no partial aggregate", 12, MUTE)
s.text(565, y2 + 40, "Each partition holds one kind (HasPartitionKey).", 11, MUTE, italic=True)
s.text(565, y2 + 56, "Spark does not check this promise.", 11, ACC, italic=True)
s.save(FIG + "ch55-partitioning.svg")

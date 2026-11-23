"""Figures for Chapter 91. Results from code/ch91/output/boundaries.txt, repro.txt and test_fix.txt."""
from svgkit import *

# 1. The boundary table, on 4.3.0 and with the patched classes
cases = ["1 USE other (has its own sales)", "2 USE empty (no sales table)", "3 USE default, temp view named sales",
         "4 source qualified as default.sales", "5 SQL source SELECT * FROM sales", "6 subquery in filter: FROM regions"]
mv = ["(east, 999)", "TABLE_OR_VIEW_NOT_FOUND", "(north, 5)", "(east, 10), (west, 20)", "(east, 999)", "(west, 20)"]
ok = "(east, 10), (west, 20)"
s = Svg(780, 300, "The six boundary cases: metric view on 4.3.0, metric view with the fix, regular view")
cols = [("metric view, 4.3.0", 300), ("metric view, patched", 460), ("regular view", 620)]
for lab, x in cols:
    s.text(x + 75, 24, lab, 12, ACC if "patched" in lab else MUTE, "middle", 700)
for i, c in enumerate(cases):
    y = 40 + i * 38
    s.text(290, y + 19, c, 11, MUTE, "end", 600)
    for k, (lab, x) in enumerate(cols):
        v = mv[i] if k == 0 else ok
        good = v == ok
        s.box(x, y, 150, 28, v, GREEN if good else RED, 10 if len(v) > 20 else 11, mono=True)
s.text(12, 286, "Expected everywhere: (east, 10), (west, 20). Cases 2 to 6 with USE other unless the row says otherwise.", 11, MUTE, "start", italic=True)
s.save(FIG + "ch91-boundaries.svg")

# 2. Where the unqualified name goes (repro.txt)
s = Svg(780, 300, "The unqualified name sales in the metric view, resolved in two namespaces")
s.frame(12, 40, 360, 140, "namespace default", MUTE, 13)
s.box(30, 72, 140, 50, "sales", BLUE, 13, mono=True, sub="(east, 10), (west, 20)", subsize=10)
s.box(215, 72, 140, 50, "mv", PURPLE, 13, mono=True, sub="metric view, source: sales", subsize=9)
s.box(215, 128, 140, 42, "v", GRAY, 13, mono=True, sub="regular view, FROM sales", subsize=9)
s.frame(408, 40, 360, 140, "namespace other", MUTE, 13)
s.box(520, 72, 140, 50, "sales", ORANGE, 13, mono=True, sub="(east, 999)", subsize=10)
s.line(213, 97, 173, 97, GREEN, 2.5)
s.path("M285,70 C300,0 560,0 590,68", RED, 2.5)
s.text(590, 150, "4.3.0, after USE other:", 11, RED, "middle", 700)
s.text(590, 165, "the session namespace", 11, RED, "middle", 700)
s.text(20, 204, "with the fix: the creation namespace (view.desc)", 12, GREEN, "start", 700)
s.text(20, 220, "→ (east, 10), (west, 20)", 12, GREEN, "start", 700)
s.text(420, 204, "4.3.0: the metric view reads other.sales", 12, RED, "start", 700)
s.text(420, 220, "→ (east, 999), and no error", 12, RED, "start", 700)
s.text(12, 260, "The regular view v always resolves in default: ViewResolution uses", 11, MUTE, "start", italic=True)
s.text(12, 276, "AnalysisContext.withAnalysisContext(view.desc). The metric view placeholder did not get this context.", 11, MUTE, "start", italic=True)
s.save(FIG + "ch91-namespaces.svg")

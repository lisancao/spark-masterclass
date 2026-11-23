"""Figures for Chapter 88. Numbers and plan lines from code/ch88/output/twice.txt and eval_types.txt."""
from svgkit import *

# 1. UDF calls of the filter on the UDF column (twice.txt)
s = Svg(780, 300, "How many times the UDF ran in two queries with a filter on the UDF column")
def flow(x, title, steps, total, rows):
    s.text(x + 170, 22, title, 13, ACC, "middle", 700)
    y = 40
    for lab, n, c in steps:
        s.box(x, y, 340, 40, lab, c, 12, sub=n, subsize=11)
        y += 50
        if (lab, n, c) != steps[-1]:
            s.line(x + 170, y - 10, x + 170, y - 1, MUTE, 2)
    s.text(x + 170, y + 14, f"UDF calls: {total}, rows returned: {rows}", 13, MUTE, "middle", 700)
flow(20, "filter on area only", [("scan: 200 rows", "no status filter to push", GRAY), ("filter copy of the UDF", "runs on 200 rows", ORANGE),
                                 ("70 rows pass", "area = 'api'", GRAY), ("projection copy of the UDF", "runs on 70 rows", ORANGE)], "200 + 70 = 270", 70)
flow(420, "filter on area and status (the slow query)", [("scan: 98 rows", "status = 200 pushed to Parquet", BLUE), ("filter copy of the UDF", "runs on 98 rows", ORANGE),
                                 ("31 rows pass", "area = 'api'", GRAY), ("projection copy of the UDF", "runs on 31 rows", ORANGE)], "98 + 31 = 129", 31)
s.text(390, 290, "Counts from an accumulator in the UDF. avoidDoubleFilterEval = true does not cover a Python UDF in 4.3.0.", 11, MUTE, "middle", italic=True)
s.save(FIG + "ch88-twice.svg")

# 2. When UDFs share one Python operator (eval_types.txt)
cases = [("nested, same type", "upper(area(path))", [["_area → <lambda>  (101)"]]),
         ("siblings, same type", "area(path), upper(ip)", [["_area, <lambda>  (101)"]]),
         ("nested, 101 over 200", "area(strip_q(path))", [["_area  (101)"], ["strip_q  (200)"]]),
         ("JVM expression between", "upper(concat(area(path), '!'))", [["<lambda>(concat(...))  (101)"], ["_area  (101)"]]),
         ("iterator siblings", "strip_q_iter(path), strip_q_iter(ip)", [["strip_q_iter(ip)  (204)"], ["strip_q_iter(path)  (204)"]]),
         ("same call twice", "area(path), area(path)", [["_area  (101)"]])]
s = Svg(780, 360, "The ArrowEvalPython operators that ExtractPythonUDFs made for six expressions")
for i, (lab, expr, ops) in enumerate(cases):
    col, row = i % 3, i // 3
    x, y = 12 + col * 256, 16 + row * 165
    s.text(x + 120, y + 14, lab, 13, ACC, "middle", 700)
    s.text(x + 120, y + 32, expr, 10, MUTE, "middle", mono=True)
    for k, op in enumerate(ops):
        s.box(x, y + 42 + k * 44, 240, 36, op[0], ORANGE, 11, mono=True)
    n = len(ops)
    s.text(x + 120, y + 140, f"{n} operator" + ("s" if n > 1 else "") + (": one trip to Python" if n == 1 else ": two trips"), 11, GREEN if n == 1 else RED, "middle", 700)
s.text(12, 352, "Each orange box is one ArrowEvalPython node. The number is the eval type.", 11, MUTE, "start", italic=True)
s.save(FIG + "ch88-fusion.svg")

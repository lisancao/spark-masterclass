"""Write catalyst/data.js: the four plans of the DataFrame query (rdd_vs_df.txt) and the codegen stage (codegen.txt)."""
import json
import re
from pathlib import Path

here = Path(__file__).resolve().parent
txt = (here.parent / "output" / "rdd_vs_df.txt").read_text()
sections = {}
for name in ("Parsed Logical Plan", "Analyzed Logical Plan", "Optimized Logical Plan", "Physical Plan"):
    body = txt.split(f"== {name} ==\n", 1)[1].split("\n\n", 1)[0].splitlines()
    nodes = []
    for line in body:
        m = re.match(r"^([ |+\-]*)(.*)$", line)
        depth = len(m[1]) // 3
        node = m[2]
        if not node or ":" in node.split(" ")[0] and depth == 0 and not node.startswith("'"):
            continue                                    # the schema line of the analyzed plan
        node = re.sub(r"#\d+L?", "", node)              # drop expression IDs for the screen
        node = node.replace("Some(America/Los_Angeles)", "tz")
        if node.startswith("Relation ["):
            node = f"Relation [{node.count(',') + 1} columns] parquet"
        if node.startswith("FileScan parquet"):
            cols = re.search(r"FileScan parquet \[([^\]]*)\]", node)[1]
            node = f"FileScan parquet [{cols}]"
        nodes.append({"depth": depth, "text": node[:58] + ("..." if len(node) > 58 else "")})
    sections[name] = nodes
cg = (here.parent / "output" / "codegen.txt").read_text()
sub1 = cg.split("== Subtree 1 / 3", 1)[1].split("\n    \n", 1)[0].splitlines()[1:]
fused = [re.sub(r"#\d+L?", "", l.strip(" +-"))[:50] for l in sub1 if "*(1)" in l]
data = {"source": "code/ch03/output/rdd_vs_df.txt, codegen.txt", "plans": sections, "fused": fused}
(here / "catalyst" / "data.js").write_text("window.DATA = " + json.dumps(data) + ";\n")
for k, v in sections.items():
    print(k); [print("  " * n["depth"] + n["text"]) for n in v]
print(fused)

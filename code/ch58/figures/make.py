"""Chapter 58 figures. Numbers from code/ch58/output/inference.txt and calling.txt."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.figkit import Fig  # noqa: E402

CHECK = Path(sys.argv[1]) if len(sys.argv) > 1 else None


def runs():
    rows = [("collect()", 240, "si", "every row"), ("count()", 0, "ok", "column pruning removes the UDF"),
            ("limit(3).collect()", 3, "ok", "the limit goes below the UDF"),
            ("filter on the label", 468, "bad", "two ArrowEvalPython: 240 + 228"),
            ("asNondeterministic() + filter", 240, "si", "the optimizer does not copy the UDF"),
            ("cache(), count(), then filter", 240, "si", "240 to fill the cache, then 0")]
    f = Fig(900, 300, "Rows that go into the model for six actions on the same 240 rows")
    f.text(20, 26, "Rows into the model, for each action on the same labelled DataFrame (240 rows)", size=14, weight=700)
    X0, S = 280, 0.75
    for i, (a, n, c, why) in enumerate(rows):
        y = 46 + i * 38
        f.text(X0 - 10, y + 18, a, size=12, mono=True, anchor="end")
        if n:
            f.rect(X0, y + 4, n * S, 22, c=c, fill=0.4, rx=3, stroke=0)
        f.text(X0 + n * S + 8, y + 20, f"{n}", size=12, mono=True, weight=700)
        f.text(X0 + n * S + 44, y + 20, why, size=11, c="mute")
    return f.save("ch58-runs", CHECK)


def requests():
    f = Fig(900, 330, "Requests to the service: one per row against a client per partition, and the rate per task")
    f.text(20, 26, "232 texts to label", size=14, weight=700)
    X0, S = 250, 1.6
    bars = [("one request per row (@udf)", [(16, "ok", "200"), (216, "bad", "429")], "14 texts labelled"),
            ("mapInPandas, 10 items a request", [(34, "ok", "200")], "228 texts labelled, 4 'no text'")]
    for i, (lab, parts, res) in enumerate(bars):
        y = 50 + i * 50
        f.text(X0 - 10, y + 20, lab, size=12, anchor="end")
        x = X0
        S1 = 1.2
        for n, c, st in parts:
            f.rect(x, y + 4, n * S1, 26, c=c, fill=0.45, rx=2, stroke=0)
            if n * S1 > 70:
                f.text(x + n * S1 / 2, y + 22, f"{n} x {st}", size=12, anchor="middle", weight=600)
            x += n * S1
        f.text(x + 8, y + 22, f"{sum(p[0] for p in parts)} requests ({', '.join(str(p[0]) + ' x ' + p[2] for p in parts)}): {res}", size=12)
    f.text(20, 176, "The rate limit is per task: total rate = per-task rate x concurrent tasks (service limit: 16/s)",
           size=14, weight=700)
    rows = [("4 partitions x 2.0 req/s", 8, 35, 0), ("16 partitions x 2.0 req/s", 32, 78, 51),
            ("16 partitions x 0.5 req/s", 8, 45, 0)]
    for i, (lab, tot, req, rej) in enumerate(rows):
        y = 194 + i * 40
        f.text(X0 - 10, y + 20, lab, size=12, anchor="end", mono=True)
        f.rect(X0, y + 4, (req - rej) * S, 26, c="ok", fill=0.45, rx=2, stroke=0)
        if rej:
            f.rect(X0 + (req - rej) * S, y + 4, rej * S, 26, c="bad", fill=0.45, rx=2, stroke=0)
        f.text(X0 + req * S + 8, y + 22, f"up to {tot}/s: {req} requests, {rej} x 429", size=12,
               c="bad" if rej else "ink")
    return f.save("ch58-requests", CHECK)


if __name__ == "__main__":
    for fn in (runs, requests):
        print(fn())

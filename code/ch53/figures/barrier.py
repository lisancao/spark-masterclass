"""Chapter 53, Step 12 figure: the barrier timeline. Numbers from code/ch53/output/barrier.txt.
Run from code/:  uv run python ch53/figures/barrier.py [png-check-dir]"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.figkit import Fig  # noqa: E402

CHECK = Path(sys.argv[1]) if len(sys.argv) > 1 else None
TASKS = [(0, 0.00, 0.00, 1.01), (1, 0.00, 0.20, 1.20), (2, 0.00, 0.40, 1.41), (3, 0.01, 0.61, 1.61)]


def barrier():
    f = Fig(900, 300, "Four barrier tasks: start, arrival at allGather, exit")
    f.text(20, 28, "RDD.barrier().mapPartitions: four tasks and one allGather (barrier.py)", size=14, weight=700)
    X0, S = 110, 420
    for t in (0, 0.5, 1.0, 1.5):
        x = X0 + t * S
        f.line(x, 50, x, 240, c="line", w=0.8, dash="3 4")
        f.text(x, 258, f"{t:.1f} s", size=11, anchor="middle", c="mute", mono=True)
    xo = X0 + 0.61 * S
    f.line(xo, 46, xo, 240, c="ok", w=2)
    f.text(xo + 6, 58, "barrier opens: the last task arrives (0.61 s)", size=12, c="ok", weight=600)
    for i, (n, start, arrive, leave) in enumerate(TASKS):
        y = 80 + i * 40
        f.text(X0 - 12, y + 15, f"task {n}", size=12, anchor="end", mono=True)
        f.rect(X0 + start * S, y, (arrive - start) * S + 0.5, 20, c="si", fill=0.7, rx=2, stroke=0)
        f.rect(X0 + arrive * S, y, (leave - arrive) * S, 20, c="go", fill=0.25, rx=2, stroke=0)
        f.circle(X0 + leave * S, y + 10, 5, c="go")
        f.text(X0 + leave * S + 10, y + 15, f"left {leave:.2f} s", size=11, mono=True)
    f.rect(110, 272, 14, 14, c="si", fill=0.7, rx=2, stroke=0)
    f.text(130, 284, "work (sleep 0.2 s x partition)", size=12)
    f.rect(360, 272, 14, 14, c="go", fill=0.25, rx=2, stroke=0)
    f.text(380, 284, "waits in allGather: checks every 1 s (Thread.sleep(1000))", size=12)
    return f.save("ch53-barrier", CHECK)


if __name__ == "__main__":
    print(barrier())

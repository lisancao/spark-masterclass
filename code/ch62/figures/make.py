"""Chapter 62 figures. From PipelineExecution.scala (v4.3.0), the Step 8 table and code/ch62/output/end_to_end.txt."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.figkit import Fig  # noqa: E402

CHECK = Path(sys.argv[1]) if len(sys.argv) > 1 else None


def five_steps():
    steps = [("1. resolve", "DataflowGraph.resolve", ["a name or a column", "that does not exist"], "si"),
             ("2. validate", "GraphValidations", ["a cycle (reported first),", "a dataset without a flow"], "si"),
             ("3. reset", "State.reset", ["a new checkpoint version", "for a full refresh"], "go"),
             ("4. materialize", "DatasetManager", ["TRUNCATE of materialized", "views, the MERGE check"], "go"),
             ("5. execute", "TriggeredGraphExecution", ["retries, SKIPPED,", "AvailableNow queries"], "go")]
    f = Fig(900, 250, "The five steps of StartRun, and what each step causes")
    W, G = 164, 15
    for i, (t, cls, effects, c) in enumerate(steps):
        x = 20 + i * (W + G)
        f.box(x, 40, W, 62, t, cls, c=c, size=14)
        if i < 4:
            f.line(x + W, 71, x + W + G - 2, 71, arrow=True)
        for j, e in enumerate(effects):
            f.text(x + 4, 130 + j * 16, e, size=11, c="mute")
    xs = 20 + 2 * (W + G) - G / 2
    f.line(xs, 28, xs, 176, c="bad", w=2, dash="6 4")
    f.text(xs - 6, 24, "a dry run stops here", size=12, c="bad", weight=600, anchor="end")
    f.text(xs + 6, 24, "writes to the catalog and the storage", size=12, c="go", weight=600)
    f.text(20, 206, "Steps 1 and 2 only analyze the plans. Steps 3 to 5 change checkpoints and tables.", size=12)
    f.text(20, 226, "Thus, a dry run cannot find an error of steps 4 or 5 (for example the empty table after a failed gate).",
           size=12, c="mute")
    return f.save("ch62-five-steps", CHECK)


def sink_log():
    f = Fig(900, 300, "Why a full refresh does not send the sink rows again")
    f.text(20, 26, "Flow shipped_orders_to_feed writes to the JSON sink exports/shipping_feed", size=14, weight=700)
    f.text(20, 60, "checkpoint", size=13, weight=600)
    f.text(20, 76, "(pipeline storage)", size=11, c="mute")
    f.text(20, 160, "file sink log", size=13, weight=600)
    f.text(20, 176, "_spark_metadata", size=11, c="mute", mono=True)
    runs = [("run 1", "version 0, batch 0", "2 rows", "batch 0 written", "ok"),
            ("run 2", "version 0, batch 1", "1 new row", "batch 1 written", "ok"),
            ("full refresh", "version 1, batch 0 again", "all shipped orders", "batch 0 is in the log: skipped", "bad")]
    for i, (r, ck, rows, res, c) in enumerate(runs):
        x = 170 + i * 240
        f.text(x + 105, 46, r, size=13, weight=700, anchor="middle", c="go" if i == 2 else "ink")
        f.box(x, 54, 210, 50, ck, rows, c="si", size=12)
        f.line(x + 105, 104, x + 105, 142, c=c, arrow=True)
        f.box(x, 146, 210, 44, res, None, c=c, size=12)
    f.text(20, 230, "The feed has 3 rows after run 2 and still 3 rows after the full refresh.", size=12)
    f.text(20, 250, "The full refresh gives the flow a new checkpoint version, and the batch IDs start again at 0.", size=12,
           c="mute")
    f.text(20, 270, "The sink log already has batches 0 and 1, so FileStreamSink.addBatch skips them.", size=12, c="mute")
    return f.save("ch62-sink-log", CHECK)


if __name__ == "__main__":
    for fn in (five_steps, sink_log):
        print(fn())

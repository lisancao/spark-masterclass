"""Chapter 63, Step 13: TorchDistributor on CPU. Two processes train one model with DistributedDataParallel.

local_mode=True: torchrun starts the processes on the driver. local_mode=False: Spark starts them as
the tasks of one barrier stage. Run from code/ (the CPU wheel of torch, not in pyproject.toml):
  uv run --with torch --index https://download.pytorch.org/whl/cpu --index-strategy unsafe-best-match \
      python ch63/torch_dist.py
"""
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.book import session, ui_api, TRIPS  # noqa: E402

PATH = str(next(Path(TRIPS).glob("*.parquet")))


def train(path, epochs=3):
    """Runs in each of the num_processes processes. torchrun sets RANK, WORLD_SIZE, MASTER_ADDR/PORT."""
    import os, socket, time
    import pyarrow.parquet as pq
    import pyarrow.compute as pc
    import torch
    import torch.distributed as dist
    from torch.nn.parallel import DistributedDataParallel as DDP

    dist.init_process_group("gloo")                                 # CPU backend
    rank, world = dist.get_rank(), dist.get_world_size()
    t = pq.read_table(path, columns=["trip_distance", "fare_amount"])
    t = t.filter(pc.and_(pc.greater(t["trip_distance"], 0.1), pc.less(t["trip_distance"], 50)))
    t = t.filter(pc.and_(pc.greater(t["fare_amount"], 1), pc.less(t["fare_amount"], 200)))
    x = torch.tensor(t["trip_distance"].to_numpy(), dtype=torch.float32)[rank::world].unsqueeze(1)
    y = torch.tensor(t["fare_amount"].to_numpy(), dtype=torch.float32)[rank::world].unsqueeze(1)
    torch.manual_seed(0)
    model = DDP(torch.nn.Linear(1, 1))
    opt = torch.optim.SGD(model.parameters(), lr=0.002)
    t0 = time.time()
    for epoch in range(epochs):
        for i in range(0, len(x), 4096):
            opt.zero_grad()
            loss = torch.nn.functional.mse_loss(model(x[i:i + 4096]), y[i:i + 4096])
            loss.backward()                                         # DDP averages the gradients here
            opt.step()
    w, b = model.module.weight.item(), model.module.bias.item()
    info = [None] * world
    dist.all_gather_object(info, (rank, os.getpid(), torch.__version__, len(x)))
    dist.destroy_process_group()
    # Every rank returns the same dict. In local mode, each rank writes its value to one file, and
    # the last writer wins (not always rank 0), so a value from rank 0 only can come back as None.
    return {"returned_by": rank, "weight": round(w, 3), "bias": round(b, 3), "last_batch_mse": round(loss.item(), 2),
            "ranks": info, "train_s": round(time.time() - t0, 1)}


if __name__ == "__main__":
    from pyspark.ml.torch.distributor import TorchDistributor
    import torch
    remote = bool(os.environ.get("SPARK_REMOTE"))
    spark = session("ch63-torch", **({} if remote else {"spark_master": "local[4]",
                                                         "spark_ui_showConsoleProgress": "false"}))
    print(f"torch {torch.__version__}, pyspark {spark.version}, driver pid {os.getpid()}, "
          f"{'Connect' if remote else 'Classic'} session")
    for local in (True, False):
        if not remote:
            spark.sparkContext.setJobDescription(f"TorchDistributor local_mode={local}")
        t0 = time.time()
        r = TorchDistributor(num_processes=2, local_mode=local, use_gpu=False).run(train, PATH)
        print(f"\n== local_mode={local}: {time.time() - t0:.1f} s in all, the value came from rank {r['returned_by']}")
        print(f"   fare = {r['weight']} * miles + {r['bias']}   (last batch mse {r['last_batch_mse']}, "
              f"training loop {r['train_s']} s)")
        for rank, pid, tv, n in r["ranks"]:
            print(f"   rank {rank}: pid {pid}, torch {tv}, {n:,} rows")
    if not remote:
        jobs = [j for j in ui_api("jobs") if "TorchDistributor" in (j.get("description") or "")]
        for j in sorted(jobs, key=lambda j: j["jobId"]):
            print(f"\nSpark jobs: '{j['description']}' -> job {j['jobId']}, {j['numTasks']} tasks, stages {j['stageIds']}")
        st = [s for s in ui_api("stages") if s["stageId"] in sum((j["stageIds"] for j in jobs), [])]
        for s in st:
            print(f"   stage {s['stageId']}: {s['name'].split(' at ')[0]}, {s['numTasks']} tasks, {s['status']}")
    spark.stop()

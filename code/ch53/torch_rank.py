"""Chapter 53, Step 13: which rank's return value does TorchDistributor.run() give back?

Run from code/ (CPU torch as in torch_dist.py):  uv run --with torch ... python ch53/torch_rank.py
"""
import collections
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.ml.torch.distributor import TorchDistributor  # noqa: E402
from common.book import session  # noqa: E402


def who():
    import os
    return int(os.environ["RANK"])


spark = session("ch53-torch-rank", spark_master="local[4]", spark_ui_showConsoleProgress="false")
for local in (True, False):
    c = collections.Counter(TorchDistributor(num_processes=2, local_mode=local, use_gpu=False).run(who)
                            for _ in range(8))
    print(f"local_mode={local!s:<5}  8 runs, the returned value came from: "
          + ", ".join(f"rank {k} x{v}" for k, v in sorted(c.items())))
spark.stop()

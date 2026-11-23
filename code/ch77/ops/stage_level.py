"""Chapter 77, Step 15: stage-level scheduling with a ResourceProfile on a standalone cluster.

Stage A runs with the default profile (CPU executors). Stage B asks for executors with one GPU each
and one GPU for each task. Spark starts new executors for the GPU profile (dynamic allocation) and
gives each task the address of its GPU.
"""
import json
import time
import urllib.request

from pyspark import TaskContext
from pyspark.resource import ExecutorResourceRequests, ResourceProfileBuilder, TaskResourceRequests
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ch77-stage-level").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
api = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}"


def where(_):
    tc = TaskContext.get()
    gpu = tc.resources().get("gpu")
    yield (tc.stageId(), tc.partitionId(), gpu.addresses if gpu else [], tc.cpus())


def show(label, rows):
    print(label)
    for stage, part, gpus, cpus in sorted(rows, key=lambda r: r[1]):
        print(f"    stage {stage}  partition {part}  cpus {cpus}  gpu addresses {gpus}")


rdd = sc.parallelize(range(4), 4)
show("stage A, default profile:", rdd.mapPartitions(where).collect())

ereq = ExecutorResourceRequests().cores(2).memory("1g").resource("gpu", 2, "/opt/getGpus.sh")
treq = TaskResourceRequests().cpus(1).resource("gpu", 1)
gpu_profile = ResourceProfileBuilder().require(ereq).require(treq).build
print(f"profile {gpu_profile.id}: executor {dict((k, v.amount) for k, v in gpu_profile.executorResources.items())}"
      f"  task {dict((k, v.amount) for k, v in gpu_profile.taskResources.items())}")
show("stage B, GPU profile:", rdd.withResources(gpu_profile).mapPartitions(where).collect())

with urllib.request.urlopen(f"{api}/allexecutors") as r:
    for e in sorted(json.load(r), key=lambda e: e["id"]):
        if e["id"] != "driver":
            res = {k: v["addresses"] for k, v in e.get("resources", {}).items()}
            print(f"executor {e['id']}  profile {e['resourceProfileId']}  cores {e['totalCores']}  "
                  f"resources {res}  active {e['isActive']}")
spark.stop()

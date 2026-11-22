#!/usr/bin/env bash
# Submit jobs/zone_revenue.py to the kind cluster "ch48" in cluster mode, scheduled by YuniKorn.
#   ch48/spark_k8s.sh <app name> <queue> [gang]
# spark-submit comes from the Spark 4.3.0 tarball (Chapter 2). It returns when the driver pod exists.
# With "gang", the driver pod also carries YuniKorn task groups: the job's minimum (one driver and
# one executor) is reserved together by placeholder pods before any real pod starts.
# With "gang3", the minimum is one driver and two executors: 3 cores, more than the queue root.etl allows.
set -euo pipefail
NAME=$1 QUEUE=$2 GANG=${3:-}
SPARK_HOME=${SPARK_TARBALL:-$HOME/spark-masterclass/data/tarball/spark-4.3.0-bin-hadoop3}
API=$(kubectl config view --context kind-ch48 --minify -o jsonpath='{.clusters[0].cluster.server}')
K=spark.kubernetes
args=(
  --master "k8s://$API" --deploy-mode cluster --name "$NAME"
  --conf $K.namespace=spark-jobs --conf $K.authenticate.driver.serviceAccountName=spark
  --conf $K.container.image=apache/spark:4.3.0-python3 --conf $K.container.image.pullPolicy=Never
  --conf $K.submission.waitAppCompletion=false
  --conf spark.executor.instances=2 --conf spark.executor.cores=1 --conf spark.driver.cores=1
  --conf spark.executor.memory=1g --conf spark.driver.memory=1g
  # YuniKorn: the scheduler name, the queue, and one application ID for the driver and its executors
  --conf $K.scheduler.name=yunikorn
  --conf $K.driver.label.queue="$QUEUE" --conf $K.executor.label.queue="$QUEUE"
  --conf $K.driver.annotation.yunikorn.apache.org/app-id='{{APP_ID}}'
  --conf $K.executor.annotation.yunikorn.apache.org/app-id='{{APP_ID}}'
  # the taxi data on every node, as a read-only hostPath volume
  --conf $K.driver.volumes.hostPath.taxi.mount.path=/data/nyc-taxi
  --conf $K.driver.volumes.hostPath.taxi.options.path=/data/nyc-taxi
  --conf $K.executor.volumes.hostPath.taxi.mount.path=/data/nyc-taxi
  --conf $K.executor.volumes.hostPath.taxi.options.path=/data/nyc-taxi
  --conf $K.driver.volumes.hostPath.jobs.mount.path=/opt/ch48/jobs
  --conf $K.driver.volumes.hostPath.jobs.options.path=/opt/ch48/jobs
)
if [[ $GANG == gang* ]]; then
  MIN_EXEC=$([[ $GANG == gang3 ]] && echo 2 || echo 1)
  # One driver and MIN_EXEC executors, each 1 CPU and 1433Mi (1g of heap plus 40%: the overhead factor for Python jobs).
  groups='[{"name":"driver","minMember":1,"minResource":{"cpu":"1","memory":"1433Mi"}},
           {"name":"executor","minMember":'$MIN_EXEC',"minResource":{"cpu":"1","memory":"1433Mi"}}]'
  args+=(
    --conf $K.driver.annotation.yunikorn.apache.org/task-group-name=driver
    --conf $K.driver.annotation.yunikorn.apache.org/task-groups="$(echo $groups | tr -d ' ')"
    --conf $K.driver.annotation.yunikorn.apache.org/schedulingPolicyParameters="placeholderTimeoutInSeconds=120 gangSchedulingStyle=Hard"
    --conf $K.executor.annotation.yunikorn.apache.org/task-group-name=executor
  )
fi
exec "$SPARK_HOME/bin/spark-submit" "${args[@]}" \
  local:///opt/ch48/jobs/zone_revenue.py /data/nyc-taxi - "$NAME"

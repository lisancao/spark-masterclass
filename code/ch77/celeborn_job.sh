#!/usr/bin/env bash
# Run jobs/shuffle_idle.py on the kind cluster "ch77" with dynamic allocation, with or without Celeborn.
#   ch77/celeborn_job.sh <celeborn|tracking> <spark version> [idle seconds]
#     celeborn   shuffle through Celeborn 0.7.0, shuffle tracking off
#     tracking   Spark's own shuffle, dynamic allocation with shuffle tracking (the default)
# spark-submit runs inside the same Spark image as the pods (apache/spark:<version>-scala2.13-java17-python3-ubuntu),
# so the client and the driver have the same Spark version. It waits for the application to end,
# then prints the driver's SHUFFLE lines.
set -euo pipefail
MODE=$1 VERSION=$2 IDLE=${3:-45}
IMAGE=apache/spark:$VERSION-scala2.13-java17-python3-ubuntu
JAR=/opt/ch77/jars/celeborn-client-spark-4-shaded_2.13-0.7.0.jar
KCFG=$(mktemp); kind get kubeconfig --name ch77 > "$KCFG"; chmod 644 "$KCFG"
API=$(kubectl config view --kubeconfig "$KCFG" --minify -o jsonpath='{.clusters[0].cluster.server}')
NAME=shuffle-$MODE-${VERSION//./}
K=spark.kubernetes
args=(
  --master "k8s://$API" --deploy-mode cluster --name "$NAME"
  --conf $K.namespace=spark-jobs --conf $K.authenticate.driver.serviceAccountName=spark
  --conf $K.container.image="$IMAGE" --conf $K.container.image.pullPolicy=Never
  --conf spark.executor.cores=1 --conf spark.executor.memory=1g --conf spark.driver.memory=1g
  --conf spark.kubernetes.executor.request.cores=500m --conf spark.kubernetes.driver.request.cores=500m
  --conf spark.dynamicAllocation.enabled=true
  --conf spark.dynamicAllocation.minExecutors=0 --conf spark.dynamicAllocation.maxExecutors=2
  --conf spark.dynamicAllocation.initialExecutors=2
  --conf spark.dynamicAllocation.executorIdleTimeout=15s
  --conf spark.sql.shuffle.partitions=8
  --conf $K.driver.volumes.hostPath.taxi.mount.path=/data/nyc-taxi
  --conf $K.driver.volumes.hostPath.taxi.options.path=/data/nyc-taxi
  --conf $K.executor.volumes.hostPath.taxi.mount.path=/data/nyc-taxi
  --conf $K.executor.volumes.hostPath.taxi.options.path=/data/nyc-taxi
  --conf $K.driver.volumes.hostPath.jobs.mount.path=/opt/ch77/jobs
  --conf $K.driver.volumes.hostPath.jobs.options.path=/opt/ch77/jobs
)
if [[ $MODE == celeborn ]]; then
  args+=(
    # the Celeborn client jar on the driver and executor class paths (a hostPath volume on every node)
    --conf $K.driver.volumes.hostPath.jars.mount.path=/opt/ch77/jars
    --conf $K.driver.volumes.hostPath.jars.options.path=/opt/ch77/jars
    --conf $K.executor.volumes.hostPath.jars.mount.path=/opt/ch77/jars
    --conf $K.executor.volumes.hostPath.jars.options.path=/opt/ch77/jars
    --conf spark.driver.extraClassPath=$JAR --conf spark.executor.extraClassPath=$JAR
    # the settings from the Celeborn 0.7.0 README
    --conf spark.shuffle.manager=org.apache.spark.shuffle.celeborn.SparkShuffleManager
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer
    --conf spark.celeborn.master.endpoints=celeborn-master-0.celeborn-master-svc.celeborn.svc.cluster.local:9097
    --conf spark.shuffle.service.enabled=false
    --conf spark.celeborn.client.push.replicate.enabled=true
    --conf spark.sql.adaptive.localShuffleReader.enabled=false
    --conf spark.shuffle.sort.io.plugin.class=org.apache.spark.shuffle.celeborn.CelebornShuffleDataIO
    --conf spark.dynamicAllocation.shuffleTracking.enabled=false
  )
fi
kubectl --context kind-ch77 -n spark-jobs delete pods -l spark-app-name="$NAME" --wait=true >/dev/null 2>&1 || true
docker run --rm --network host -v "$KCFG:/tmp/kubeconfig:ro" -e KUBECONFIG=/tmp/kubeconfig "$IMAGE" \
  /opt/spark/bin/spark-submit "${args[@]}" local:///opt/ch77/jobs/shuffle_idle.py /data/nyc-taxi "$IDLE" \
  2>&1 | grep -E "phase: |termination reason|exit code" | grep -v "^\s*$" | tail -3 >&2 || true
rm -f "$KCFG"
log=$(kubectl --context kind-ch77 -n spark-jobs logs "$(kubectl --context kind-ch77 -n spark-jobs get pods \
  -l spark-app-name="$NAME",spark-role=driver -o name | tail -1)")
grep -E "^SHUFFLE" <<<"$log" || grep -m1 -E "^[a-zA-Z.]+(Exception|Error):" <<<"$log" | fold -s -w 92
if [[ $MODE == celeborn ]]; then   # what the Celeborn master logged for this application
  app=$(kubectl --context kind-ch77 -n spark-jobs get pods -l spark-app-name="$NAME",spark-role=driver \
    -o jsonpath='{.items[-1:].metadata.labels.spark-app-selector}')
  kubectl --context kind-ch77 -n celeborn logs celeborn-master-0 -c celeborn | grep "$app" \
    | sed -E 's/^[0-9/]+ ([0-9:]+),[0-9]+ INFO \[[^]]+\] /CELEBORN \1  /; s/spark-([0-9a-f]{8})[0-9a-f]{24}/spark-\1…/' || true
fi

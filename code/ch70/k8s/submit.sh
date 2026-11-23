#!/usr/bin/env bash
# Chapter 70, Steps 6 to 8: native Spark on Kubernetes with spark-submit, no operator.
#   ch70/k8s/submit.sh no-rbac | cluster | pods | client        (from code/, kind cluster up)
set -uo pipefail
cd "$(dirname "$0")"
export BOOK_DATA=${BOOK_DATA:-$HOME/spark-masterclass/data}
export SPARK_HOME=$BOOK_DATA/tarball/spark-4.3.0-bin-hadoop3
export SPARK_CONF_DIR=$(mktemp -d)          # no spark-defaults.conf: every setting is on the command line
CTX=kind-ops80 NS=spark-apps
K="kubectl --context $CTX -n $NS"
API=$(kubectl config view -o jsonpath="{.clusters[?(@.name=='$CTX')].cluster.server}")
COMMON=(
  --master "k8s://$API"
  --conf spark.kubernetes.context=$CTX
  --conf spark.kubernetes.namespace=$NS
  --conf spark.kubernetes.container.image=spark-masterclass/spark:4.3.0-book
  --conf spark.kubernetes.container.image.pullPolicy=Never
  --conf spark.executor.instances=2
  --conf spark.executor.memory=1g
  --conf spark.kubernetes.executor.request.cores=500m
  --conf spark.kubernetes.driver.request.cores=500m
  --conf spark.kubernetes.executor.volumes.hostPath.taxi.mount.path=/data/nyc-taxi
  --conf spark.kubernetes.executor.volumes.hostPath.taxi.mount.readOnly=true
  --conf spark.kubernetes.executor.volumes.hostPath.taxi.options.path=/data/nyc-taxi
)
DRIVER_VOLUMES=(
  --conf spark.kubernetes.driver.volumes.hostPath.taxi.mount.path=/data/nyc-taxi
  --conf spark.kubernetes.driver.volumes.hostPath.taxi.mount.readOnly=true
  --conf spark.kubernetes.driver.volumes.hostPath.taxi.options.path=/data/nyc-taxi
  --conf spark.kubernetes.driver.volumes.hostPath.code.mount.path=/opt/ch70
  --conf spark.kubernetes.driver.volumes.hostPath.code.mount.readOnly=true
  --conf spark.kubernetes.driver.volumes.hostPath.code.options.path=/opt/ch70
)
story() { grep -v -E "WARNING: Using incubator|NativeCodeLoader|^\s*$" | sed -E 's/^[0-9/]+ ([0-9:]+) (INFO|WARN|ERROR) /\1 \2 /'; }
case ${1:-cluster} in
no-rbac)      # the namespace's default service account may not create pods
  $K delete pod -l spark-app-name=ch70-no-rbac --ignore-not-found >/dev/null
  "$SPARK_HOME/bin/spark-submit" "${COMMON[@]}" "${DRIVER_VOLUMES[@]}" --deploy-mode cluster --name ch70-no-rbac \
    --conf spark.kubernetes.submission.waitAppCompletion=false \
    local:///opt/ch70/app.py /data/nyc-taxi/yellow 2>&1 | grep -E "Deployed|submission ID" | story
  D=$($K get pod -l spark-app-name=ch70-no-rbac,spark-role=driver -o name)
  for _ in $(seq 60); do $K logs "$D" 2>/dev/null | grep -q "forbidden" && break; sleep 2; done
  echo "--- driver log"; $K logs "$D" | grep -m1 -o -E "Failure executing: [A-Z]+ at: [^ ]+\. Message: [^.]*\." | sed 's/ Message: /\nMessage: /' | fold -s -w 92
  echo "--- driver pod: $($K get "$D" -o jsonpath='{.status.phase}')  service account: $($K get "$D" -o jsonpath='{.spec.serviceAccountName}')"
  $K delete "$D" --wait=false >/dev/null ;;
cluster)      # cluster mode: the driver runs in a pod, with the RBAC service account and a pod template
  $K delete pod -l spark-app-name=ch70-k8s --ignore-not-found >/dev/null
  "$SPARK_HOME/bin/spark-submit" "${COMMON[@]}" "${DRIVER_VOLUMES[@]}" --deploy-mode cluster --name ch70-k8s \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-driver \
    --conf spark.kubernetes.executor.podTemplateFile=executor-template.yaml \
    --conf spark.kubernetes.driver.label.book.chapter=80 \
    local:///opt/ch70/app.py /data/nyc-taxi/yellow --hold 25 2>&1 \
    | grep -E "^\s+phase:|Application ch70-k8s with" | story | uniq > /tmp/ch70-submit.$$ &
  SUB=$!
  for _ in $(seq 120); do
    n=$($K get pod -l spark-role=executor --no-headers 2>/dev/null | grep -c " Running ")
    [ "$n" -ge 2 ] && break; sleep 1
  done
  sleep 2
  echo "--- what Spark created (while the application runs)"
  $K get pods,cm -o custom-columns='KIND:.kind,NAME:.metadata.name,ROLE:.metadata.labels.spark-role,STATUS:.status.phase,NODE:.spec.nodeName'
  $K get svc -o custom-columns='SERVICE:.metadata.name,CLUSTER-IP:.spec.clusterIP,PORTS:.spec.ports[*].port' | grep ch70-k8s
  E=$($K get pod -l spark-role=executor -o name | head -1)
  echo "--- $E: fields from the template"
  $K get "$E" -o jsonpath='{"label book.chapter:      "}{.metadata.labels.book\.chapter}{"\n"}{"annotation book/template: "}{.metadata.annotations.book/template}{"\n"}{"env BOOK_TEMPLATE:       "}{.spec.containers[0].env[?(@.name=="BOOK_TEMPLATE")].value}{"\n"}{"volume spark-local-dir-1: "}{.spec.volumes[?(@.name=="spark-local-dir-1")].emptyDir}{"\n"}'
  echo "--- $E: fields that Spark set"
  $K get "$E" -o jsonpath='{"container:        "}{.spec.containers[0].name}{"\n"}{"image:            "}{.spec.containers[0].image}{"\n"}{"args:             "}{.spec.containers[0].args}{"\n"}{"resources:        "}{.spec.containers[0].resources}{"\n"}{"SPARK_LOCAL_DIRS: "}{.spec.containers[0].env[?(@.name=="SPARK_LOCAL_DIRS")].value}{"\n"}{"service account:  "}{.spec.serviceAccountName}{"\n"}{"owner:            "}{.metadata.ownerReferences[0].kind}/{.metadata.ownerReferences[0].name}{"\n"}'
  wait $SUB
  echo "--- spark-submit, which waits for the driver pod (spark.kubernetes.submission.waitAppCompletion=true)"
  cat /tmp/ch70-submit.$$; rm -f /tmp/ch70-submit.$$
  D=$($K get pod -l spark-app-name=ch70-k8s,spark-role=driver -o name)
  echo "--- driver stdout ($D)"; $K logs "$D" 2>/dev/null | grep -E "^(driver|master|deploy|app id|executor|trips)"
  echo "--- pods after the end"; $K get pods --no-headers -o custom-columns='NAME:.metadata.name,STATUS:.status.phase'
  $K delete pod --all --wait=false >/dev/null ;;
client|client-default)   # client mode: the driver runs here, the executors in kind call back to this machine
  EXTRA=(--conf spark.kubernetes.executor.useDriverPodIP=false)   # (4.1+) default true: executors dial the bind address
  [[ $1 == client-default ]] && EXTRA=(--conf spark.kubernetes.executor.deleteOnTermination=false)
  GW=$(docker network inspect kind -f '{{range .IPAM.Config}}{{.Gateway}} {{end}}' | tr ' ' '\n' | grep -E '^[0-9]+\.' | head -1)
  "$SPARK_HOME/bin/spark-submit" "${COMMON[@]}" --deploy-mode client --name ch70-k8s-client \
    --conf spark.driver.host="$GW" --conf spark.driver.bindAddress=0.0.0.0 \
    --conf spark.driver.port=7178 --conf spark.driver.blockManager.port=7179 \
    --conf spark.ui.port=4151 \
    --conf spark.kubernetes.executor.volumes.hostPath.taxi.mount.path="$BOOK_DATA/nyc-taxi" \
    --conf spark.kubernetes.executor.podTemplateFile=executor-template.yaml \
    --conf spark.pyspark.python=python3 "${EXTRA[@]}" \
    --conf spark.pyspark.driver.python="$(cd ../.. && pwd)/.venv/bin/python" \
    ../app.py "$BOOK_DATA/nyc-taxi/yellow" > /tmp/ch70-client.$$ 2>&1 &
  SUB=$!
  for _ in $(seq 200); do
    kill -0 $SUB 2>/dev/null || break
    E=$($K get pod -l spark-role=executor -o name 2>/dev/null | head -1)
    [[ -n $E ]] && $K logs "$E" 2>/dev/null | grep -q -E "timed out|Failed to connect" && break
    grep -q "^trips" /tmp/ch70-client.$$ && break
    sleep 2
  done
  grep -E "^(driver|master|deploy|app id|executor|trips)|ERROR" /tmp/ch70-client.$$ | story
  if [[ -n ${E:-} ]] && kill -0 $SUB 2>/dev/null; then
    echo "--- $E log"; $K logs "$E" | grep -m1 -E "timed out|Failed to connect" | story
    kill $SUB; wait $SUB 2>/dev/null; $K delete pod --all --wait=false >/dev/null
  fi
  rm -f /tmp/ch70-client.$$
  ;;
in-cluster)   # client mode with the driver in a pod of its own and a headless service
  $K delete -f client-driver.yaml --ignore-not-found --wait=true >/dev/null 2>&1
  $K apply -f client-driver.yaml >/dev/null
  for _ in $(seq 150); do
    ph=$($K get pod ch70-client-driver -o jsonpath='{.status.phase}' 2>/dev/null)
    [[ $ph == Succeeded || $ph == Failed ]] && break
    [[ -z ${shown:-} ]] && [ "$($K get pod -l spark-role=executor --no-headers 2>/dev/null | grep -c " Running ")" -ge 2 ] && {
      shown=1; echo "--- pods while the driver pod runs"
      $K get pods -o custom-columns='NAME:.metadata.name,ROLE:.metadata.labels.spark-role,OWNER:.metadata.ownerReferences[0].name,STATUS:.status.phase'; }
    sleep 1
  done
  echo "--- driver pod $ph; its stdout"
  $K logs ch70-client-driver | grep -E "^(driver|master|deploy|app id|executor|trips)"
  $K delete -f client-driver.yaml --wait=false >/dev/null ;;
esac
rm -rf "$SPARK_CONF_DIR"

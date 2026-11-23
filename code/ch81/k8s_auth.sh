#!/usr/bin/env bash
# Chapter 81, Step 2: spark.authenticate on Kubernetes. Spark makes one secret per application and
# gives it to the executors in the pod spec. Runs Chapter 80's app.py. Uses Chapter 80's kind cluster (ch80/k8s/cluster.sh up).
#   ch81/k8s_auth.sh env | file        (from code/)
set -uo pipefail
cd "$(dirname "$0")"
export BOOK_DATA=${BOOK_DATA:-$HOME/spark-masterclass/data}
SPARK_HOME=$BOOK_DATA/tarball/spark-4.3.0-bin-hadoop3
export SPARK_CONF_DIR=$(mktemp -d)
CTX=kind-ops80 NS=spark-apps K="kubectl --context kind-ops80 -n spark-apps"
API=$(kubectl config view -o jsonpath="{.clusters[?(@.name=='$CTX')].cluster.server}")
EXTRA=()
if [[ ${1:-env} == file ]]; then   # the secret from a Kubernetes Secret, mounted as a file in both pods
  $K delete secret spark-auth --ignore-not-found >/dev/null
  $K create secret generic spark-auth --from-literal=secret="$(python3 -c 'import secrets;print(secrets.token_hex(32))')" >/dev/null
  EXTRA=(--conf spark.kubernetes.driver.secrets.spark-auth=/etc/spark-auth
         --conf spark.kubernetes.executor.secrets.spark-auth=/etc/spark-auth
         --conf spark.authenticate.secret.driver.file=/etc/spark-auth/secret
         --conf spark.authenticate.secret.executor.file=/etc/spark-auth/secret)
fi
$K delete pod --all --wait=true >/dev/null 2>&1
"$SPARK_HOME/bin/spark-submit" --master "k8s://$API" --deploy-mode cluster --name ch81-k8s-auth \
  --conf spark.kubernetes.context=$CTX --conf spark.kubernetes.namespace=$NS \
  --conf spark.kubernetes.container.image=spark-masterclass/spark:4.3.0-book \
  --conf spark.kubernetes.container.image.pullPolicy=Never \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-driver \
  --conf spark.executor.instances=2 --conf spark.kubernetes.executor.request.cores=500m \
  --conf spark.kubernetes.driver.request.cores=500m \
  --conf spark.kubernetes.driver.volumes.hostPath.code.mount.path=/opt/ch80 \
  --conf spark.kubernetes.driver.volumes.hostPath.code.options.path=/opt/ch80 \
  --conf spark.kubernetes.driver.volumes.hostPath.taxi.mount.path=/data/nyc-taxi \
  --conf spark.kubernetes.driver.volumes.hostPath.taxi.options.path=/data/nyc-taxi \
  --conf spark.kubernetes.executor.volumes.hostPath.taxi.mount.path=/data/nyc-taxi \
  --conf spark.kubernetes.executor.volumes.hostPath.taxi.options.path=/data/nyc-taxi \
  --conf spark.authenticate=true --conf spark.network.crypto.enabled=true \
  --conf spark.kubernetes.submission.waitAppCompletion=false "${EXTRA[@]}" --conf spark.kubernetes.executor.deleteOnTermination=false \
  local:///opt/ch80/app.py /data/nyc-taxi/yellow --hold 15 >/dev/null 2>&1
for _ in $(seq 120); do
  [ "$($K get pod -l spark-role=executor --no-headers 2>/dev/null | grep -c " Running ")" -ge 2 ] && break; sleep 1
done
E=$($K get pod -l spark-role=executor -o name | head -1)
echo "mode: ${1:-env}"
echo "executor pod env, as anyone with 'get pods' in the namespace reads it:"
$K get "$E" -o json | python3 podenv.py
echo "executor volumes: $($K get "$E" -o jsonpath='{range .spec.volumes[*]}{.name} {end}')"
D=$($K get pod -l spark-role=driver -o name)
for _ in $(seq 60); do [[ $($K get "$D" -o jsonpath='{.status.phase}') == Succeeded ]] && break; sleep 2; done
echo "driver: $($K logs "$D" | grep "^trips")   (pod $($K get "$D" -o jsonpath='{.status.phase}'))"
if [[ ${1:-env} == file ]]; then
  E=$($K get pod -l spark-role=executor -o name | head -1)
  echo "executor log: $($K logs "$E" 2>/dev/null | grep -m1 -o "IllegalArgumentException: .*" | cut -c1-100)"
fi
$K delete pod --all --wait=false >/dev/null; $K delete secret spark-auth --ignore-not-found >/dev/null
rm -rf "$SPARK_CONF_DIR"

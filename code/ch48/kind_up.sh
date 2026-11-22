#!/usr/bin/env bash
# Create (or delete) Chapter 48's kind cluster "ch48" and load the Spark images into it.
#   ch48/kind_up.sh [create|delete]
# It also downloads the Celeborn 0.7.0 client for Spark 4 from Maven Central (checked against its SHA-1).
set -euo pipefail
CH48=$(cd "$(dirname "$0")" && pwd)
DATA=${BOOK_DATA:-$HOME/spark-masterclass/data}
TAXI=${CH48_TAXI:-$DATA/nyc-taxi}
JARS=$DATA/ch48/jars
IMAGES=(apache/spark:4.3.0-python3                              # Spark 4.3.0, Java 21: Airflow, YuniKorn, operator
        apache/spark:4.2.1-scala2.13-java17-python3-ubuntu      # Spark 4.2.1, Java 17: Celeborn 0.7.0 supports it
        apache/spark:4.3.0-scala2.13-java17-python3-ubuntu      # Spark 4.3.0, Java 17: the unsupported Celeborn test
        apache/celeborn:0.7.0)
if [[ ${1:-create} == delete ]]; then kind delete cluster --name ch48; exit 0; fi

CLB=https://repo1.maven.org/maven2/org/apache/celeborn/celeborn-client-spark-4-shaded_2.13/0.7.0
JAR=celeborn-client-spark-4-shaded_2.13-0.7.0.jar
mkdir -p "$JARS"
if [[ ! -f $JARS/$JAR ]]; then
  curl -fsSL "$CLB/$JAR" -o "$JARS/$JAR"
  echo "$(curl -fsSL "$CLB/$JAR.sha1")  $JARS/$JAR" | sha1sum -c -
fi

cfg=$(mktemp)
sed -e "s#__TAXI__#$TAXI#" -e "s#__JOBS__#$CH48/jobs#" -e "s#__JARS__#$JARS#" "$CH48/k8s/kind-ch48.yaml" > "$cfg"
kind create cluster --config "$cfg" --image kindest/node:v1.37.0 --wait 120s
rm -f "$cfg"
for image in "${IMAGES[@]}"; do
  docker image inspect "$image" >/dev/null 2>&1 || docker pull -q "$image"
  kind load docker-image "$image" --name ch48
done
kubectl --context kind-ch48 apply -f "$CH48/k8s/rbac.yaml"
kubectl --context kind-ch48 get nodes

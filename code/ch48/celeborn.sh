#!/usr/bin/env bash
# Install Apache Celeborn 0.7.0 into the kind cluster "ch48" with its Helm chart (from the v0.7.0 source tag).
#   ch48/celeborn.sh
# The chart is not in a Helm repository, so the script fetches charts/celeborn from the release tag.
set -euo pipefail
CH48=$(cd "$(dirname "$0")" && pwd)
WORK=${CH48_WORK:-$HOME/.cache/spark-masterclass/ch48}
SRC=$WORK/celeborn-v0.7.0
if [[ ! -d $SRC/charts/celeborn ]]; then
  git clone -q --depth 1 --branch v0.7.0 --filter=blob:none --sparse https://github.com/apache/celeborn.git "$SRC"
  git -C "$SRC" sparse-checkout set charts
fi
docker image inspect alpine:3.18 >/dev/null 2>&1 || docker pull -q alpine:3.18
kind load docker-image alpine:3.18 --name ch48 >/dev/null     # the chart's init container
helm --kube-context kind-ch48 upgrade --install celeborn "$SRC/charts/celeborn" \
  --namespace celeborn --create-namespace -f "$CH48/k8s/celeborn-values.yaml" --wait --timeout 5m
kubectl --context kind-ch48 -n celeborn get pods -o wide | awk '{print $1, $2, $3, $7}' | column -t

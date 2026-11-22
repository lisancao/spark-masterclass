#!/usr/bin/env bash
# Chapter 47, Steps 1 and 2: what the install made, and the Spark configs that the operator added
# to the driver of sparkapp/zones (from the driver's config map).
#   ch47/inspect.sh        (from code/, after ch47/run_app.py zones-app.yaml)
set -euo pipefail
K="kubectl --context kind-part08"
echo "--- kind nodes";  $K get nodes --no-headers -o custom-columns=NAME:.metadata.name,VERSION:.status.nodeInfo.kubeletVersion
echo "--- helm release"; helm --kube-context kind-part08 list --no-headers | awk '{print $1, $9, $10}'
echo "--- CRDs";        $K get crd --no-headers | awk '/spark.apache.org/ {print $1}'
echo "--- operator pod"; $K get pods -l app.kubernetes.io/name=spark-kubernetes-operator --no-headers \
  -o custom-columns=NAME:.metadata.name,IMAGE:.spec.containers[0].image,STATUS:.status.phase
echo "--- short names"; $K api-resources --api-group=spark.apache.org --no-headers | awk '{print $1, $2}'
echo "--- configs in the driver config map of sparkapp/zones (selected)"
$K get configmap zones-0-spark-drv-conf-map -o jsonpath='{.data.spark\.properties}' \
  | grep -E '^spark\.(authenticate|io\.encryption\.enabled|master|app\.id|submit\.pyFiles|kubernetes\.executor\.deleteOnTermination|kubernetes\.driver\.annotateExitException|kubernetes\.executor\.useDriverPodIP|kubernetes\.container\.image)=' | sort

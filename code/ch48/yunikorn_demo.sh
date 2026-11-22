#!/usr/bin/env bash
# Submit two Spark jobs at the same time into YuniKorn's queue root.etl (2 cores), and watch their pods.
#   ch48/yunikorn_demo.sh plain [seconds]   no gang: each driver starts, then waits for executors
#   ch48/yunikorn_demo.sh gang  [seconds]   gang: each job reserves its driver and one executor together
#   ch48/yunikorn_demo.sh gang3 [seconds]   a gang of 3 cores, larger than the queue: YuniKorn rejects it
set -euo pipefail
cd "$(dirname "$0")/.."
MODE=$1 SECONDS_TO_WATCH=${2:-90}
k() { kubectl --context kind-ch48 -n spark-jobs "$@"; }
k delete pods --all --wait=true >/dev/null 2>&1 || true
GANG=$([[ $MODE == gang* ]] && echo $MODE || true)
ch48/spark_k8s.sh etl-a root.etl $GANG >/dev/null 2>&1 &
ch48/spark_k8s.sh etl-b root.etl $GANG >/dev/null 2>&1 &
wait
if [[ $MODE == gang ]]; then       # keep one placeholder pod as YuniKorn made it
  python3 ch48/watch_pods.py "$SECONDS_TO_WATCH" --until-done &
  until k get pods -o name | grep -q '^pod/tg-.*executor'; do sleep 0.5; done
  k get pods -o json | jq '[.items[] | select(.metadata.name | test("^tg-.*executor"))][0]
    | {name: .metadata.name, labels: .metadata.labels, scheduler: .spec.schedulerName,
       annotations: (.metadata.annotations | del(."yunikorn.apache.org/task-groups")),
       image: .spec.containers[0].image, requests: .spec.containers[0].resources.requests}' \
    > ch48/output/yunikorn_placeholder.json
  wait
  k logs "$(k get pods -o name | grep 'etl-a.*driver')" | grep ZONE_REVENUE > ch48/output/yunikorn_gang_driver_log.txt
  exit 0
fi
if [[ $MODE == gang3 ]]; then     # YuniKorn rejects the application: show the pods and the reason
  sleep 10
  k get pods
  pod=$(k get pods -o name | grep 'etl-a.*driver')
  k get events --field-selector involvedObject.name="${pod#pod/}",reason=ApplicationFailed \
    -o jsonpath='{range .items[*]}{.message}{"\n"}{end}' | fold -s -w 92
  exit 0
fi
python3 ch48/watch_pods.py "$SECONDS_TO_WATCH" --until-done
if [[ $MODE == plain ]]; then      # why the jobs wait: the scheduler's event and the driver's log
  pod=$(k get pods -o name | grep 'etl-a.*exec-1' | head -1)
  k get events --field-selector involvedObject.name="${pod#pod/}" -o jsonpath='{range .items[*]}{.message}{"\n"}{end}' \
    | grep 'does not fit' | tail -1 | sed -E 's/ephemeral-storage:[0-9]+ //; s/(Request )[^ ]+/\1<exec-1>/' | fold -s -w 92
  k logs "$(k get pods -o name | grep 'etl-a.*driver')" | grep 'Initial job' | tail -1 | cut -c19- | fold -s -w 92
  k delete pods --all --wait=false >/dev/null      # nothing will change: stop the two jobs
fi

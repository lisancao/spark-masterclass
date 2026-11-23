# Spark Masterclass, Chapter 62: the Spark Kubernetes Operator. Storyboard: notes/part08.md.
#   needs the kind cluster from ch62/cluster.sh up
#   film render code/film/ch62.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 BOOK_DATA=$HOME/spark-masterclass/data SPARK_UI_PORT=4028 PATH=$FILM_DEMO/.venv/bin:$PATH && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: app | A SparkApplication through the operator's states
Type "# a PySpark SparkApplication on kind, followed through the operator's states" Enter
Sleep 500ms
Type "python ch62/run_app.py zones-app.yaml 2>/dev/null | cut -c1-94" Enter
Wait@300s
Sleep 12s

# beat: connect | A Connect server in the cluster, a client on the laptop, a driver restart
Hide
Type "kubectl --context kind-part08 delete sparkapp connect --wait=true >/dev/null; kubectl --context kind-part08 apply -f ch62/connect-server.yaml >/dev/null; until kubectl --context kind-part08 get sparkapp connect -o jsonpath='{.status.currentState.currentStateSummary}' | grep -q RunningHealthy; do sleep 2; done; sleep 5; clear" Enter
Wait@300s
Show
Type "# a laptop client, a Connect server in the cluster, then the driver pod dies" Enter
Sleep 500ms
Type "python ch62/connect_k8s.py 2>/dev/null | cut -c1-94" Enter
Wait@400s
Sleep 12s

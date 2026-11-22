# Spark Masterclass, Chapter 63: Orchestrating Spark. Storyboard: storyboard.md (ch63.tape).
#   film render code/film/ch63.tape --format chapter
# Needs: the Airflow venv (ch63/airflow_env.sh), the kind cluster "ch63" with YuniKorn and Celeborn.
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4064 && setopt interactivecomments" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: submit | Airflow runs spark-submit, then spark-sql
Type "# a DAG of two Spark 4.3 tasks: SparkSubmitOperator, then SparkSqlOperator" Enter
Sleep 500ms
Type "ch63/af.sh dags test ch63_taxi_submit 2>&1 | python3 ch63/airflow_log.py | cut -c1-94" Enter
Wait@300s
Sleep 8s

# beat: connect | A task with no JVM, over Spark Connect
Hide
Type "ch63/connect_server.sh start >/dev/null; clear" Enter
Wait@120s
Show
Type "# @task.pyspark with a spark_connect connection: a thin client, no JVM in the task" Enter
Sleep 500ms
Type "ch63/af.sh dags test ch63_taxi_connect 2>&1 | python3 ch63/airflow_log.py" Enter
Wait@300s
Sleep 2s
Type "# new in 4.3: the server's REST API shows the task's session and operation" Enter
Sleep 500ms
Type "uv run python ch63/connect_rest.py" Enter
Wait@60s
Sleep 8s

# beat: plain | Two Spark jobs in a small queue, no gang
Type "# two jobs in YuniKorn queue root.etl (2 cores), no gang scheduling" Enter
Sleep 500ms
Type "ch63/yunikorn_demo.sh plain 40" Enter
Wait@300s
Sleep 8s

# beat: gang | The same two jobs, with gang scheduling
Type "# the same two jobs, each with a gang: one driver and one executor, reserved together" Enter
Sleep 500ms
Type "ch63/yunikorn_demo.sh gang 240" Enter
Wait@400s
Sleep 8s

# beat: celeborn | Dynamic allocation with Celeborn, Spark 4.2.1
Type "# Spark 4.2.1 + Celeborn 0.7.0: executors go idle, then the same action runs again" Enter
Sleep 500ms
Type "ch63/celeborn_job.sh celeborn 4.2.1 45 2>/dev/null | cut -c1-94" Enter
Wait@400s
Sleep 8s

# beat: celeborn43 | The same Celeborn client on Spark 4.3.0
Type "# the same Celeborn 0.7.0 client on Spark 4.3.0 (no Celeborn release supports 4.3)" Enter
Sleep 500ms
Type "ch63/celeborn_job.sh celeborn 4.3.0 45 2>/dev/null" Enter
Wait@300s
Sleep 8s

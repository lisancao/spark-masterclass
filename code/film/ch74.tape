# Spark Masterclass, Chapter 74: Setting Spark configurations. VO lines: notes/v2-config-sql.md.
#   film render code/film/ch74.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4142 CONNECT_PORT=15102 PYTHONUNBUFFERED=1 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: layers | One key, set in more places each time
Hide
Type "clear" Enter
Show
Type "# a new session per row, with one more config source each time" Enter
Sleep 500ms
Type "python ch74/layers.py 2>/dev/null | sed -n '2,11p'" Enter
Wait@300s
Sleep 8s

# beat: static | Static configs refuse a change at runtime
Hide
Type "clear" Enter
Show
Type "# spark.conf.set on a runtime, a static and a core config" Enter
Sleep 500ms
Type "python ch74/static_runtime.py 2>/dev/null | sed -n '/== 2/,/== 3/p' | head -14" Enter
Wait@300s
Sleep 8s

# beat: connect | What a Connect client may set
Hide
Type "ch74/connect_server.sh start >/dev/null; clear" Enter
Wait@120s
Show
Type "# two Connect sessions on one server: each has its own runtime configs" Enter
Sleep 500ms
Type "python ch74/connect_conf.py 2>/dev/null | sed -n '/== 1/,/== 3/p' | head -12" Enter
Wait@120s
Sleep 8s
Hide
Type "ch74/connect_server.sh stop" Enter
Wait@60s

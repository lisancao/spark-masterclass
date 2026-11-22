# Spark Masterclass, Chapter 51: Shipping a source. Storyboard: storyboard-part09a.md.
#   film render code/film/ch51.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4029 BOOK_DATA=$HOME/spark-masterclass/data CH51_VENVS=/tmp/claude-1000/-home-lnc-repos/ba9be504-7f98-4ba6-9537-bf1ce27aaf62/scratchpad/part09a/venvs && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show


# beat: workers | Which Python do the workers use?
Hide
Type "clear" Enter
Show
Type "# the package is in 'with': first PYSPARK_PYTHON unset, then the workers use 'bare'" Enter
Sleep 500ms
Type "$CH51_VENVS/with/bin/python ch51/registration.py lookup 2>/dev/null; PYSPARK_PYTHON=$CH51_VENVS/bare/bin/python $CH51_VENVS/with/bin/python ch51/registration.py lookup 2>/dev/null" Enter
Wait@300s
Sleep 12s

# beat: logs | Worker logs in a table
Hide
Type "clear" Enter
Show
Type "# logging and print() inside read(), read back from python_worker_logs()" Enter
Sleep 500ms
Type "PYSPARK_PYTHON=$CH51_VENVS/with/bin/python $CH51_VENVS/with/bin/python ch51/registration.py classic 2>/dev/null | sed -n '/^python_worker_logs/,$p'" Enter
Wait@300s
Sleep 12s

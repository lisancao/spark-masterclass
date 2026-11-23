# Spark Masterclass, Chapter 75: testing and debugging PySpark. Storyboard: notes/v3-ml-testing.md.
#   film render code/film/ch75.tape --format chapter   (start ch64/server.sh first)
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 BOOK_DATA=$HOME/spark-masterclass/data SPARK_UI_PORT=4158 SPARK_LOCAL_IP=127.0.0.1 && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: pytest | The same tests on a Connect server, then four ways they fail
Type "# run the tests against a Connect server: one session fixture for the whole run" Enter
Sleep 500ms
Type "MODE=connect SPARK_CONNECT_URL=sc://localhost:15118 uv run -q --with pytest pytest -q -p no:cacheprovider ch75/tests 2>/dev/null | tail -2" Enter
Wait@300s
Sleep 2s
Type "# now break the setup four ways: where is the UDF built, does the worker see fares.py?" Enter
Sleep 500ms
Type "uv run -q --with pytest python ch75/traps.py 2>/dev/null | cut -c1-92" Enter
Wait@600s
Sleep 10s

# beat: debugger | A breakpoint inside a Python worker
Type "clear" Enter
Type "# breakpoint() in a UDF: the worker waits on a TCP port, pdb commands go over the socket" Enter
Sleep 500ms
Type "uv run -q --with remote-pdb python ch75/remote_debug.py 2>/dev/null" Enter
Wait@300s
Sleep 10s

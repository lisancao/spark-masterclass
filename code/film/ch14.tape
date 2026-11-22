# Spark Masterclass, Chapter 14: Procedural SQL. VO lines: notes/part02.md.
#   film render code/film/ch14.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4122 BOOK_NO_LOCK=1 PYTHONUNBUFFERED=1 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: loop | One script, many queries
Hide
Type "clear" Enter
Show
Type "# a 5-pass loop in one SQL script: how many queries and jobs?" Enter
Sleep 500ms
Type "python ch14/scripting.py 2>/dev/null | sed -n '/== 4/,/== 6/{/== 6/q;p}' | cut -c1-92" Enter
Wait@300s
Sleep 10s

# beat: udf | A SQL UDF is inlined
Hide
Type "clear" Enter
Show
Type "# the same function in SQL and in Python: what is in the plan?" Enter
Sleep 500ms
Type "python ch14/functions.py 2>/dev/null | sed -n '/analyzed plan/,/== 3/{/== 3/q;p}' | cut -c1-92" Enter
Wait@300s
Sleep 10s

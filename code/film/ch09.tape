# Spark Masterclass, Chapter 9: DataFrames, columns and schemas. VO lines: notes/part02.md.
#   film render code/film/ch09.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4122 BOOK_NO_LOCK=1 PYTHONUNBUFFERED=1 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: drift | Schema drift against a contract
Hide
Type "clear" Enter
Show
Type "# three days of events: what changed against the schema contract?" Enter
Sleep 500ms
Type "python ch09/drift.py 2>/dev/null | sed -n '/== 5/,/== 6/{/== 6/q;p}' | cut -c1-92" Enter
Wait@300s
Sleep 10s

# beat: zones | Two timestamp types, two time zones
Hide
Type "clear" Enter
Show
Type "# TIMESTAMP is an instant, TIMESTAMP_NTZ is a wall-clock time" Enter
Sleep 500ms
Type "python ch09/type_system.py 2>/dev/null | sed -n '/== 3/,/== 4/{/== 4/q;p}' | cut -c1-92" Enter
Wait@300s
Sleep 10s

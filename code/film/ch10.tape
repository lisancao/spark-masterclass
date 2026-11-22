# Spark Masterclass, Chapter 10: Cleaning values. VO lines: notes/part02.md.
#   film render code/film/ch10.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4122 BOOK_NO_LOCK=1 PYTHONUNBUFFERED=1 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: ansi | ANSI mode stops the job
Hide
Type "clear" Enter
Show
Type "# one zero fare in 3.8 million trips: ANSI on and off" Enter
Sleep 500ms
Type "python ch10/ansi.py 2>/dev/null | sed -n '1,/== 3/{/== 3/q;p}' | cut -c1-92" Enter
Wait@300s
Sleep 10s

# beat: nanos | Nanosecond timestamps, a 4.3 preview
Hide
Type "clear" Enter
Show
Type "# new in 4.3: nanosecond timestamps from Parquet" Enter
Sleep 500ms
Type "python ch10/nanos.py 2>/dev/null | sed -n '/== 2/,/== 3/{/== 3/q;p}' | cut -c1-92" Enter
Wait@300s
Sleep 10s

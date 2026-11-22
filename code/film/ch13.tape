# Spark Masterclass, Chapter 13: Spark SQL in depth. VO lines: notes/part02.md.
#   film render code/film/ch13.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4122 BOOK_NO_LOCK=1 PYTHONUNBUFFERED=1 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: frames | The default frame includes ties
Hide
Type "clear" Enter
Show
Type "# the default frame is RANGE ... CURRENT ROW: watch the two 30s" Enter
Sleep 500ms
Type "python ch13/windows.py 2>/dev/null | sed -n '/== 4/,/== 5/{/== 5/q;p}' | cut -c1-92" Enter
Wait@300s
Sleep 10s

# beat: topn | QUALIFY and WindowGroupLimit
Hide
Type "clear" Enter
Show
Type "# top-N per group: which filters get a WindowGroupLimit?" Enter
Sleep 500ms
Type "python ch13/qualify.py 2>/dev/null | sed -n '/== 3/,/== 5/{/== 5/q;p}' | cut -c1-92" Enter
Wait@300s
Sleep 10s

# Spark Masterclass, Chapter 21: Time series and geospatial. VO lines: notes/part02.md.
#   film render code/film/ch21.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4122 BOOK_NO_LOCK=1 PYTHONUNBUFFERED=1 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: gaps | Buckets with no rows
Hide
Type "clear" Enter
Show
Type "# trips per hour in a quiet zone: 3 rows for 24 hours" Enter
Sleep 500ms
Type "python ch21/timeseries.py 2>/dev/null | sed -n '/== 5/,/== 6/{/== 6/q;p}' | cut -c1-92" Enter
Wait@300s
Sleep 10s

# beat: nearest | The nearest store, three ways
Hide
Type "clear" Enter
Show
Type "# the nearest store: all pairs, a grid and NEAREST BY" Enter
Sleep 500ms
Type "python ch22/geo_points.py 2>/dev/null | sed -n '/== 8/,/== 9/{/== 9/q;p}' | grep -v only | cut -c1-92" Enter
Wait@300s
Sleep 10s

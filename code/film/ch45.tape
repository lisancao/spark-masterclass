# Spark Masterclass, Chapter 45: Parquet from the inside.
#   film render code/film/ch45.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4154 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: footer | The bytes at the two ends of a Parquet file
Type "# PAR1, the footer length, and the Thrift footer read by hand" Enter
Sleep 500ms
Type "python ch45/footer.py 2>/dev/null | sed -n '1,27p' | cut -c1-94" Enter
Wait@180s
Sleep 12s

# beat: skipping | Rows the Parquet reader returned, layout by layout
Hide
Type "clear" Enter
Show
Type "# 2,000,000 events in four layouts: what can each filter skip?" Enter
Sleep 500ms
Type "python ch45/skipping.py layouts 2>/dev/null | sed -n '1,15p' | cut -c1-94" Enter
Wait@600s
Sleep 12s


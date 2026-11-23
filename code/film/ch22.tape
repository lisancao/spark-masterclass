# Spark Masterclass, Chapter 22: Geospatial data in Spark 4.3.
#   film render code/film/ch22.tape --format chapter
# Needs ch22/zones.py first (pip.py reads data/work/ch22/zones.parquet).
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 BOOK_DATA=$HOME/spark-masterclass/data SPARK_UI_PORT=4143 SPARK_LOCAL_IP=127.0.0.1 && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: inventory | Five ST functions in 4.3.0, and the names it does not have
Type "# Which ST functions does Spark 4.3.0 have, in SQL, PySpark and Scala?" Enter
Sleep 500ms
Type "python ch22/inventory.py 2>/dev/null | sed -n '1,5p;/== 5/,$p' | cut -c1-90" Enter
Wait@180s
Sleep 9s

# beat: pip | Point in polygon by hand: a grid key, a bounding box, then edge crossings
Hide
Type "clear" Enter
Show
Type "# Citi Bike starts, June 2026, against the 263 taxi zones. No st_contains:" Enter
Sleep 500ms
Type "python ch22/pip.py 2>/dev/null | sed -n '/^distinct/p;/== 3/,/inside bbox/p;/== 5/,/^Row/p' | cut -c1-90" Enter
Wait@300s
Sleep 10s

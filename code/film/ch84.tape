# Spark Masterclass, Chapter 84. VO lines: notes/v3-sql-ddl.md.
#   film render code/film/ch84.tape --format chapter --demo code
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4153 HMS_PORT=19113 PYTHONUNBUFFERED=1 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: overwrite | Static against dynamic partition overwrite
Hide
Type "clear" Enter
Show
Type "# INSERT OVERWRITE of June 2 into a table of three days: STATIC, then DYNAMIC" Enter
Sleep 500ms
Type "python ch84/partition_overwrite.py 2>/dev/null | grep -E '^== [12]\.|Mode =|  day=' | head -10" Enter
Wait@400s
Sleep 8s

# beat: writers | writeTo on the session catalog and on Iceberg
Hide
Type "clear" Enter
Show
Type "# DataFrameWriterV2: a V1 parquet table, then an Iceberg table" Enter
Sleep 500ms
Type "python ch84/writers.py 2>/dev/null | sed -n '/== 4/,$p' | grep -E 'ok$|failed$|v1 table|rows:' | cut -c1-90 | head -12" Enter
Wait@400s
Sleep 8s

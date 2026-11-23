# Spark Masterclass, Chapter 17. VO lines: notes/v3-sql-ddl.md.
#   film render code/film/ch17.tape --format chapter --demo code
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4153 HMS_PORT=19113 PYTHONUNBUFFERED=1 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: msck | Folders that the metastore does not know, and MSCK REPAIR TABLE
Hide
Type "clear" Enter
Show
Type "# another job writes partition folders: the Hive metastore does not see them" Enter
Sleep 500ms
Type "python ch17/partitions.py 2>/dev/null | sed -n '/== 2/,/== 3/p' | grep -v -E '^\+-|^\|partition|^$' | cut -c1-90" Enter
Wait@400s
Sleep 8s

# beat: hms | A Hive Metastore 4.0.1 in Docker, two Spark processes
Hide
Type "clear" Enter
Show
Type "# Hive Metastore 4.0.1 in Docker: the builtin 2.3.10 client, then a 4.0.1 client" Enter
Sleep 500ms
Type "python ch17/remote_metastore.py 2>/dev/null | grep -E '^==|Invalid method|^  Type|TRANSLATED|borough=|^\|[A-Z]' | cut -c1-90" Enter
Wait@600s
Sleep 8s

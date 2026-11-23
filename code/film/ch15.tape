# Spark Masterclass, Chapter 15: Spark SQL: the language, the catalog and the tools. VO lines: notes/v2-config-sql.md.
#   film render code/film/ch15.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4142 CONNECT_PORT=15102 THRIFT_PORT=15102 PYTHONUNBUFFERED=1 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: cli | The spark-sql shell
Hide
Type "clear" Enter
Show
Type "# spark-sql: a SQL shell with a Hive metastore, in one command" Enter
Sleep 500ms
Type "ch15/sql_cli.sh | sed -n '/== 1/,/== 2/p' | head -18" Enter
Wait@300s
Sleep 8s

# beat: thrift | The Thrift server, a JDBC client and a Python client
Hide
Type "clear" Enter
Show
Type "# the Thrift server: beeline and PyHive connect to one Spark driver" Enter
Sleep 500ms
Type "ch15/thrift_demo.sh | sed -n '/== 2/,$p' | grep -v -E '^\+-+\+$'" Enter
Wait@300s
Sleep 8s

# beat: catalogs | One query over two catalogs
Hide
Type "clear" Enter
Show
Type "# three-part names: a Parquet table and an H2 table in one join" Enter
Sleep 500ms
Type "python ch15/catalogs.py 2>/dev/null | sed -n '/== 5/,/== 6/p' | head -12" Enter
Wait@300s
Sleep 8s

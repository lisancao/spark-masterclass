# Spark Masterclass, Chapter 39: Databases over JDBC.
#   code/ch39/pg.sh up && uv run python ch39/load.py   (PostgreSQL in Docker with the taxi month)
#   film render code/film/ch39.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 BOOK_DATA=$HOME/spark-masterclass/data SPARK_UI_PORT=4155 SPARK_LOCAL_IP=127.0.0.1 && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: pushdown | The same GROUP BY through the jdbc format and through the JDBC catalog
Type "# One GROUP BY, two read paths. What does PostgreSQL receive?" Enter
Sleep 500ms
Type "python ch39/show_sql.py 2>/dev/null" Enter
Wait@300s
Sleep 10s

# beat: append | Each task commits its own transaction
Hide
Type "clear" Enter
Show
Type "# 8 tasks append 800 rows. Task 7 hits a duplicate key. What stays?" Enter
Sleep 500ms
Type "python ch39/writes.py 2>/dev/null | sed -n '/append 800/,/(8 rows)/p' | cut -c1-94" Enter
Wait@400s
Sleep 10s

# beat: catalog | A JDBC catalog: Spark SQL statements become PostgreSQL DDL
Hide
Type "clear" Enter
Show
Type "# CREATE NAMESPACE and CREATE TABLE in Spark, through JDBCTableCatalog:" Enter
Sleep 500ms
Type "python ch39/catalog.py 2>/dev/null | sed -n '/CREATE NAMESPACE/,/^spark-sql> INSERT/p' | cut -c1-94" Enter
Wait@300s
Sleep 10s

# Spark Masterclass, Chapter 55: Apache Iceberg with Spark. Storyboard: storyboard.md.
#   code/ch55/catalogs.sh up        (the Hive Metastore and Polaris containers)
#   film render code/film/ch55.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 BOOK_DATA=$HOME/spark-masterclass/data SPARK_UI_PORT=4063 SPARK_LOCAL_IP=127.0.0.1 && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: jar | The released runtime fails on Spark 4.3, the PR build works
Type "# Iceberg 1.12.0 has no runtime for Spark 4.3. Try the jar for Spark 4.1:" Enter
Sleep 500ms
Type "python ch55/wrong_jar.py 2>/dev/null" Enter
Wait@180s
Sleep 4s
Type "# a build of apache/iceberg PR #18749, for Spark 4.3:" Enter
Sleep 500ms
Type "python ch55/runtime.py 2>/dev/null | sed -n '/^Spark/,/iceberg_version/p'" Enter
Wait@180s
Sleep 8s

# beat: commit | One INSERT, and the files it adds to metadata/
Hide
Type "clear" Enter
Show
Type "# CREATE TABLE writes v1.metadata.json. INSERT adds data, a manifest and v2:" Enter
Sleep 500ms
Type "python ch55/tables.py 2>/dev/null | sed -n '/^== INSERT/,/summary/p'" Enter
Wait@300s
Sleep 10s

# beat: catalogs | Four catalogs in one session, four places for the pointer
Hide
Type "clear" Enter
Show
Type "# Hadoop, JDBC, Hive and REST (Polaris) catalogs, in one Spark session:" Enter
Sleep 500ms
Type "python ch55/catalogs.py 2>/dev/null | sed -n '/^== One table/,/39 endpoints/p' | cut -c1-94" Enter
Wait@300s
Sleep 12s

# beat: maintenance | The maintenance procedures, and the files on disk
# (catalogs.py removes the Hadoop warehouse, so the table is built again, off camera)
Hide
Type "clear" Enter
Type "python ch55/tables.py >/dev/null 2>&1; python ch55/evolve.py >/dev/null 2>&1; python ch55/snapshots.py >/dev/null 2>&1; clear" Enter
Wait@1500s
Show
Type "# small INSERTs, then each maintenance procedure. Count the files on disk:" Enter
Sleep 500ms
Type "python ch55/maintenance.py 2>/dev/null | grep -E '^(before|after)' | cut -c1-94" Enter
Wait@600s
Sleep 12s

# Spark Masterclass, Chapter 32: Declarative Pipelines. Storyboard: notes/part06.md (film section).
#   film render code/film/ch32.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 BOOK_DATA=${BOOK_DATA:-$HOME/spark-masterclass/data} CONNECT_PORT=10260 SPARK_UI_PORT=4126 HIVE=spark.sql.catalogImplementation=hive && setopt interactivecomments" Enter
Type "pl() { common/pipeline.sh $1 part06/$2 ${@:3} 2>&1 | grep -E 'Failed to resolve flow:|Error|Exception|Flow|Run is' | sed -E 's/^[0-9-]+ //; s/spark_catalog.default.//g' | cut -c1-92; }" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: dryrun | A dry run finds a cycle, then a wrong table name
Hide
Type "FRESH=1 common/pipeline.sh ch32/broken part06/film30-broken prepare; F=$BOOK_DATA/work/part06/film30-broken/transformations/revenue.py; clear" Enter
Sleep 1s
Show
Type "# two datasets that read each other: dry-run checks the graph, reads no data" Enter
Sleep 500ms
Type "grep -n -E 'def |spark.table' $F | sed -n '7,20p' | cut -c1-88" Enter
Sleep 2s
Type "pl ch32/broken film30-broken dry-run" Enter
Wait@120s
Sleep 6s
Type "# fix the cycle, and run the dry run again" Enter
Sleep 500ms
Type `sed -i 's/table("top_customers").groupBy/table("orders_enriched").groupBy/' $F` Enter
Type "pl ch32/broken film30-broken dry-run | grep -v upstream" Enter
Wait@120s
Sleep 8s

# beat: run | A run: each flow waits for its inputs
Hide
Type "FRESH=1 common/pipeline.sh ch32/retail part06/film30-retail prepare; clear" Enter
Sleep 1s
Show
Type "# run the retail pipeline with a Hive metastore, and read the event stream" Enter
Sleep 500ms
Type "SERVER_CONF=$HIVE pl ch32/retail film30-retail run | grep -E 'RUNNING|COMPLETED'" Enter
Wait@180s
Sleep 10s

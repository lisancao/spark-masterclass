# Spark Masterclass, Chapter 35: Python UDTFs.
#   film render code/film/ch35.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4139 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: lifecycle | One instance per partition, terminate at the end
Type "# a UDTF that logs its own lifecycle: __init__, eval, terminate, cleanup" Enter
Sleep 500ms
Type "python ch35/udtf_core.py lifecycle 2>/dev/null | cut -c1-94" Enter
Wait@180s
Sleep 10s

# beat: jsontable | JSON_TABLE against a parsing UDTF
Hide
Type "clear" Enter
Show
Type "# 20,000 JSON orders to item rows: a Python UDTF, JSON_TABLE, from_json" Enter
Sleep 500ms
Type "ORDERS=20000 RUNS=3 python ch35/json_table.py 2>/dev/null | cut -c1-94" Enter
Wait@300s
Sleep 10s

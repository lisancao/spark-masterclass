# Spark Masterclass, Chapter 31: medallion. Storyboard: notes/part06.md (film section).
#   film render code/film/ch31.tape --format chapter (after ch31/two_runs.sh and ch31/gate.sh)
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 BOOK_DATA=${BOOK_DATA:-$HOME/spark-masterclass/data} SPARK_UI_PORT=4026 W=${BOOK_DATA:-$HOME/spark-masterclass/data}/work/part06 && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: incremental | The second run read only the new file
Type "# each pipeline run is one Spark event log: read the streaming progress of bronze" Enter
Sleep 500ms
Type "python ch31/progress.py $W/ch31-shop | cut -c1-92" Enter
Sleep 12s

# beat: gate | The gate stopped silver, and silver is empty
Hide
Type "clear" Enter
Show
Type "# day 3 broke the gate: what is left in each table?" Enter
Sleep 500ms
Type "SHOW=1 python ch30/show_tables.py $W/ch31-quality quality_report silver_orders 2>/dev/null | grep -v tables" Enter
Wait@120s
Sleep 10s

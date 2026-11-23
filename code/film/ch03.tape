# Spark Masterclass, Chapter 3: from RDDs to a SQL engine. Storyboard: storyboard-background.md.
#   film render code/film/ch03.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 BOOK_DATA=$HOME/spark-masterclass/data SPARK_UI_PORT=4141 SPARK_LOCAL_IP=127.0.0.1 && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: times | The same question as RDD code and as a DataFrame
Type "# the same question, as Python RDD code and as a DataFrame" Enter
Sleep 500ms
Type "python ch03/rdd_vs_df.py 2>/dev/null | head -8" Enter
Hide
Wait@400s
Show
Sleep 10s

# beat: row | One row as Tungsten keeps it
Hide
Type "clear" Enter
Show
Type "# one taxi row, as the bytes of an UnsafeRow" Enter
Sleep 500ms
Type "python ch03/tungsten.py 2>/dev/null | head -10" Enter
Hide
Wait@300s
Show
Sleep 10s

# beat: codegen | The Java loop that whole-stage codegen wrote
Hide
Type "clear" Enter
Show
Type "# the generated Java: scan, filter, project and aggregate in one loop" Enter
Sleep 500ms
Type "python ch03/codegen.py 2>/dev/null | sed -n '/generated code/,/final plan/p' | cut -c1-94" Enter
Hide
Wait@300s
Show
Sleep 10s

# Spark Masterclass, Chapter 19: Aggregation: GROUP BY, grouping sets, ROLLUP, CUBE and pivots.
#   film render code/film/ch19.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 BOOK_DATA=$HOME/spark-masterclass/data SPARK_UI_PORT=4152 SPARK_LOCAL_IP=127.0.0.1 PYTHONUNBUFFERED=1 && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: rollup | ROLLUP: one Expand, and grouping_id for each level
Type "# ROLLUP(Borough, pay): which rows are totals, and what does the plan do?" Enter
Sleep 500ms
Type "python ch19/sets.py 2>/dev/null | sed -n '/== 1/,/== 3/{/== 3/q;p}' | grep -v Broadcast | cut -c1-92" Enter
Wait@300s
Sleep 10s

# beat: combine | New in 4.3: adjacent aggregates combine, and a sorted child gets SortAggregate
Hide
Type "clear" Enter
Show
Type "# GROUP BY after a sort-merge join on the same key: 4.3.0 against the old rules" Enter
Sleep 500ms
Type "python ch19/groupby.py 2>/dev/null | sed -n '/== 3\./,/== 3b/{/== 3b/q;p}' | cut -c1-92" Enter
Wait@300s
Sleep 10s

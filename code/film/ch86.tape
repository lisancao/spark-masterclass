# Spark Masterclass, Chapter 86: statistics, the cost-based optimizer and partition pruning.
#   film render code/film/ch86.tape --format chapter --demo code
Set TypingSpeed 40ms
Set WaitPattern /❯$/

Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4154 BOOK_DATA=$HOME/spark-masterclass/data BOOK_NO_LOCK=1 BOOK_RUNS=1 && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: estimates | ANALYZE TABLE, then the estimates with and without a histogram
Type "# what the optimizer believes about a filter, before and after ANALYZE TABLE" Enter
Sleep 500ms
Type "python ch86/stats.py 2>/dev/null | sed -n '/=== 3/,/histogram of/p' | cut -c1-92" Enter
Wait@300s
Sleep 12s

# beat: dpp | dynamic partition pruning: partitions read with and without it
Hide
Type "clear" Enter
Show
Type "# Sundays only: how many day partitions does the fact scan open?" Enter
Sleep 500ms
Type "python ch86/dpp.py 2>/dev/null | sed -n '3,15p' | cut -c1-92" Enter
Wait@600s
Sleep 12s

# beat: hints | partitioning hints and the files that a write makes
Hide
Type "clear" Enter
Show
Type "# one query, nine partitioning hints: the Exchange and the files written" Enter
Sleep 500ms
Type "python ch86/hints.py 2>/dev/null | sed -n '/=== 5/,/=== 6/p' | grep -v '^$' | cut -c1-92" Enter
Wait@600s
Sleep 12s

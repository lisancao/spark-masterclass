# Spark Masterclass, Chapter 55: a DSv2 connector in Java. Storyboard: storyboard.md.
#   film render code/film/ch55.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4030 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: capability | The same questions, to a Python source and a Java source
Type "# the same three queries, against the Python source and the Java source" Enter
Sleep 500ms
Type "python ch55/capability.py 2>/dev/null | grep -E '^==|BatchScan|LocalTableScan|CollectLimit' | grep -v formatted | cut -c1-92" Enter
Wait@180s
Sleep 12s

# beat: pushdown | Pruning, a pushed filter and a COUNT(*) with no file read
Hide
Type "clear" Enter
Show
Type "# what jdocs takes from the plan: columns, a filter, a whole COUNT(*)" Enter
Sleep 500ms
Type "python ch55/jdocs.py 2>/dev/null | sed -n '/== 4\./,/== 5\./p;/== 6\./,/== 7\./p;/== 9\./,/sum(size)/p' | grep -v '== [57]\.' | cut -c1-92" Enter
Wait@180s
Sleep 12s

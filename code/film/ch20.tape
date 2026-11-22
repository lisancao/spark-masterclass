# Spark Masterclass, Chapter 20: Python on Spark.
#   film render code/film/ch20.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4139 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: transpile | Which UDFs the 4.3 transpiler lowers
Type "# 4.3 transpiler: which Python UDFs lose their Python node?" Enter
Sleep 500ms
Type "python ch20/transpile.py 2>/dev/null | sed -n '3,15p' | cut -c1-94" Enter
Wait@180s
Sleep 10s

# beat: calls | How often Spark calls a Python UDF
Hide
Type "clear" Enter
Show
Type "# count the calls of a UDF on 1,000 rows with an accumulator" Enter
Sleep 500ms
Type "python ch20/calls.py 2>/dev/null | sed -n '1,8p'" Enter
Wait@240s
Sleep 9s

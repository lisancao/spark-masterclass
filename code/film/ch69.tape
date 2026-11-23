# Spark Masterclass, Chapter 69: calling models and services. Storyboard: notes/part07.md.
#   film render code/film/ch69.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 BOOK_DATA=$HOME/spark-masterclass/data SPARK_UI_PORT=4227 SPARK_LOCAL_IP=127.0.0.1 && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: runs | How many rows go into the model for each action
Type "# one labelled DataFrame, five actions: how many rows reach the model?" Enter
Sleep 500ms
Type "python ch69/inference.py 2>/dev/null | sed -n '/how many times/,/cache()/p' | grep -v 'filter plan' | cut -c1-92" Enter
Wait@900s
Sleep 10s

# beat: requests | One request per row, then a client per partition
Type "clear" Enter
Type "# the same 232 texts: a UDF per row, then mapInPandas with batches and a rate limit" Enter
Sleep 500ms
Type "python ch69/calling.py 2>/dev/null | sed -n '/once per row/,/largest request 10/p' | cut -c1-92" Enter
Wait@900s
Sleep 10s

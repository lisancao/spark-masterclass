# Spark Masterclass, Chapter 73: one engine for batch and streaming. Storyboard: storyboard-background.md.
#   film render code/film/ch73.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 BOOK_DATA=$HOME/spark-masterclass/data SPARK_UI_PORT=4141 SPARK_LOCAL_IP=127.0.0.1 && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: same | One function as a batch query and as a stream
Type "# one function: read the inbox as a table, then as a stream" Enter
Sleep 500ms
Type "python ch73/same_code.py 2>/dev/null | sed -n '1,22p'" Enter
Hide
Wait@300s
Show
Sleep 12s

# beat: dstream | The first streaming API, still in the package
Hide
Type "clear" Enter
Show
Type "# DStreams: the first streaming API, deprecated since 3.4" Enter
Sleep 500ms
Type "python ch73/dstream.py 2>/dev/null" Enter
Hide
Wait@180s
Show
Sleep 8s

# beat: graphs | GraphFrames 0.12.3 on Spark 4.3
Hide
Type "clear" Enter
Show
Type "# GraphFrames, the DataFrame graph package, on Spark 4.3" Enter
Sleep 500ms
Type "uv run -q --with graphframes-py==0.12.3 python ch73/graphs.py 2>/dev/null | head -22" Enter
Hide
Wait@400s
Show
Sleep 10s

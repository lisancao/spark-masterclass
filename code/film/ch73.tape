# Spark Masterclass, Chapter 73: capstone, your own connector. Storyboard: storyboard.md.
#   film render code/film/ch73.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4030 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: stream | The changes table as a stream, 400 bytes a batch
Type "# stream the change logs, 400 bytes per micro-batch, complete lines only" Enter
Sleep 500ms
Type "python ch73/demo.py 2>/dev/null | sed -n '/== 9\./,/rows streamed/p'" Enter
Wait@240s
Sleep 10s

# beat: grade | Ten stages, graded one by one
Hide
Type "clear" Enter
Show
Type "# grade the connector: parsers and tests first, then Spark" Enter
Sleep 500ms
Type "python ch73/grade.py 2>/dev/null | cut -c1-92" Enter
Wait@300s
Sleep 10s

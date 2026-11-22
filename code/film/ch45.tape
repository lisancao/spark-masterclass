# Spark Masterclass, Chapter 45: observability. Storyboard: notes/part08.md.
#   film render code/film/ch45.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 BOOK_DATA=$HOME/spark-masterclass/data SPARK_UI_PORT=4028 PATH=$FILM_DEMO/.venv/bin:$PATH && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: connect | Connect operations in the REST API, live and in the History Server
Type "# two Connect sessions, then the 4.3 REST endpoints, live and from the History Server" Enter
Sleep 500ms
Type "python ch45/connect_obs.py 2>/dev/null | grep -v 'jobTag:\|statement:\|their tags\|the tag as' | cut -c1-94" Enter
Wait@300s
Sleep 12s

# beat: listener | A Python streaming listener sees the drift in each batch
Hide
Type "clear" Enter
Show
Type "# a StreamingQueryListener with observe(): nulls in each micro-batch" Enter
Sleep 500ms
Type "python ch45/stream_listener.py 2>/dev/null | grep -v durationMs | cut -c1-94" Enter
Wait@180s
Sleep 12s

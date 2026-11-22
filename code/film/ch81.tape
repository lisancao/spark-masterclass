# Spark Masterclass, Chapter 81: Session observability. Storyboard: notes/v2-connect-core.md.
#   film render code/film/ch81.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 SPARK_LOCAL_IP=127.0.0.1 BOOK_DATA=$HOME/spark-masterclass/data CONNECT_PORT=15105 CONNECT_UI_PORT=4145 PROXY_PORT=25105 PATH=$FILM_DEMO/.venv/bin:$PATH && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: stop | Find what a client runs, and stop it from a second process
Hide
Type "rm -rf ../data/ch81-events; mkdir -p ../data/ch81-events; ch04/server.sh start --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=file://$PWD/../data/ch81-events >/dev/null; clear" Enter
Wait@120s
Show
Type "# an agent runs a long query: find it in the REST API, then interrupt it" Enter
Sleep 500ms
Type "python ch81/observe.py 2>/dev/null | sed -n '/== 2/,/== 5/p' | cut -c1-92" Enter
Wait@180s
Sleep 12s

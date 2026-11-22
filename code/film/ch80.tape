# Spark Masterclass, Chapter 80: Inside the Spark Connect protocol. Storyboard: notes/v2-connect-core.md.
#   film render code/film/ch80.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 SPARK_LOCAL_IP=127.0.0.1 BOOK_DATA=$HOME/spark-masterclass/data CONNECT_PORT=15105 CONNECT_UI_PORT=4145 PROXY_PORT=25105 PATH=$FILM_DEMO/.venv/bin:$PATH && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: proxy | Every RPC of a short session, through a logging proxy
Hide
Type "ch04/server.sh start >/dev/null; clear" Enter
Wait@120s
Show
Type "# a gRPC proxy logs each RPC between the client and the server" Enter
Sleep 500ms
Type "python ch80/protocol.py 2>/dev/null | sed -n '/client call/,/stop()/p' | cut -c1-94" Enter
Wait@180s
Sleep 12s

# beat: arrow | The Arrow IPC stream inside one response
Hide
Type "clear" Enter
Show
Type "# the rows came back as an Arrow IPC stream inside a protobuf message" Enter
Sleep 500ms
Type "python ch80/wire.py 2>/dev/null | sed -n '/== 6/,$p'" Enter
Wait@120s
Sleep 12s

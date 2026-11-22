# Spark Masterclass, Chapter 4: Spark Connect. Storyboard: storyboard.md.
#   film render code/film/ch04.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 BOOK_DATA=$HOME/spark-masterclass/data CONNECT_PORT=15105 CONNECT_UI_PORT=4145 && setopt interactivecomments && alias nvim='nvim -u $FILM_KIT/nvim-film.lua' && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: connect | A query and a mistake, over Spark Connect
Hide
Type "ch04/server.sh start >/dev/null; rm -f ../data/failed_op; clear" Enter
Wait@120s
Show
Type "# the same DataFrame code, sent to a Connect server as a plan" Enter
Sleep 500ms
Type "python ch04/connect.py 2>/dev/null | sed -n '/busiest/,/SQLSTATE/p'" Enter
Wait@120s
Sleep 10s

# beat: rest | The failed operation, from the server's REST API
Hide
Type "export API=localhost:$CONNECT_UI_PORT/api/v1/applications/$(curl -s localhost:$CONNECT_UI_PORT/api/v1/applications | jq -r '.[0].id') OP=$(cat ../data/failed_op); clear" Enter
Sleep 1s
Show
Type "# new in 4.3: look the operation up by the ID the client printed" Enter
Sleep 500ms
Type "curl -s $API/connect/operations | jq '.[] | select(.operationId == env.OP) | {operationId, state, executionTime, sqlExecIds, detail: .detail[0:120]}'" Enter
Sleep 10s

# beat: thin | The same server from pyspark-client: no jars, no Py4J
Hide
Type "clear" Enter
Show
Type "# a client environment with only pyspark-client 4.3.0: how big, and does it work?" Enter
Sleep 500ms
Type "ch04/sizes.sh /tmp/claude-1000/-home-lnc-repos/ba9be504-7f98-4ba6-9537-bf1ce27aaf62/scratchpad/connect-core" Enter
Wait@180s
Sleep 10s

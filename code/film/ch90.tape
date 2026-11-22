# Spark Masterclass, Chapter 90: RDDs today. Needs the Connect server: ch90/connect_server.sh start
#   film render code/film/ch90.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 BOOK_DATA=$HOME/spark-masterclass/data SPARK_UI_PORT=4157 SPARK_LOCAL_IP=127.0.0.1 PYTHONHASHSEED=0 && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: lazy | Transformations run nothing, an action runs a job
Type "# transformations are lazy: the first job starts at count()" Enter
Sleep 500ms
Type "python ch90/basics.py 2>/dev/null | sed -n '1,10p'" Enter
Hide
Wait@300s
Show
Sleep 8s

# beat: keyvalue | reduceByKey against groupByKey
Hide
Type "clear" Enter
Show
Type "# the same totals three ways: what crosses the shuffle?" Enter
Sleep 500ms
Type "python ch90/keyvalue.py 2>/dev/null" Enter
Hide
Wait@300s
Show
Sleep 10s

# beat: connect | No RDDs over Spark Connect
Hide
Type "clear" Enter
Show
Type "# over Spark Connect: no sparkContext, no df.rdd, but the DataFrame equivalents" Enter
Sleep 500ms
Type "SPARK_REMOTE=sc://localhost:15117 python ch90/connect.py 2>/dev/null" Enter
Hide
Wait@300s
Show
Sleep 10s

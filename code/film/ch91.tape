# Spark Masterclass, Chapter 91: pandas API on Spark. Needs the Connect server: ch90/connect_server.sh start
#   film render code/film/ch91.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 BOOK_DATA=$HOME/spark-masterclass/data SPARK_UI_PORT=4157 SPARK_LOCAL_IP=127.0.0.1 PYTHONHASHSEED=0 && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show


# beat: layers | A pandas-on-Spark DataFrame, opened
Hide
Type "clear" Enter
Show
Type "# the layers of a pandas-on-Spark DataFrame, and the plan of the default index" Enter
Sleep 500ms
Type "python ch91/internals.py 2>/dev/null | sed -n '/== 1/,/== 3/p' | grep -v spark_frame | cut -c1-94 | head -24" Enter
Hide
Wait@600s
Show
Sleep 10s

# beat: compare | pandas, pandas API on Spark, PySpark
Hide
Type "clear" Enter
Show
Type "# five operations in three APIs, on 3.8 million trips" Enter
Sleep 500ms
Type "python ch91/compare.py 2>/dev/null" Enter
Hide
Wait@600s
Show
Sleep 10s

# beat: connect | The same API over Spark Connect
Hide
Type "clear" Enter
Show
Type "# the pandas API on Spark over Spark Connect" Enter
Sleep 500ms
Type "SPARK_REMOTE=sc://localhost:15117 python ch91/api.py 2>/dev/null | sed -n '1p;/== 2/,/== 4/p' | cut -c1-94" Enter
Hide
Wait@600s
Show
Sleep 10s

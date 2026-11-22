# Spark Masterclass, Chapter 37: MLlib over Spark Connect. Storyboard: notes/part07.md.
#   film render code/film/ch37.tape --format chapter   (start ch37/server.sh first)
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 BOOK_DATA=$HOME/spark-masterclass/data SPARK_UI_PORT=4227 SPARK_LOCAL_IP=127.0.0.1 && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: connect | The fitted pipeline over Connect: references and the server ML cache
Type "# fit the fare pipeline over Spark Connect: what does the client hold?" Enter
Sleep 500ms
Type "python ch37/connect_ml.py 2>/dev/null | sed -n '/stages in/,/^ *sample/p' | cut -c1-92" Enter
Wait@900s
Sleep 10s

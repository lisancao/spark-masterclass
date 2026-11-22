# Spark Masterclass, Chapter 23: Project Feather.
#   film render code/film/ch23.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4139 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: feather | Single-task execution off and on
Type "# a small query and the 62 MB June file, single-task execution off and on" Enter
Sleep 500ms
Type "python ch23/demo.py 2>/dev/null" Enter
Wait@300s
Sleep 12s

# beat: connect | A per-session setting on a Connect server
Hide
Type "clear" Enter
Show
Type "# over Spark Connect: session a turns it on, session b keeps the default" Enter
Sleep 500ms
Type "python ch23/connect.py 2>/dev/null | sed -n '1,13p'" Enter
Wait@300s
Sleep 10s

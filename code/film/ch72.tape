# Spark Masterclass, Chapter 72: Spark Connect plugins. Storyboard: storyboard.md.
#   film render code/film/ch72.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4030 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: plugins | A relation plugin returns the plan as rows
Hide
Type "ch72/server.sh start >/dev/null; clear" Enter
Wait@180s
Show
Type "# a Connect client has no JVM: a server plugin sends the plan back as rows" Enter
Sleep 500ms
Type "python ch72/plugins.py 2>/dev/null | sed -n '/== 4\./,/== 5\./p' | grep -v '== 5' | cut -c1-92" Enter
Wait@180s
Sleep 12s

# beat: artifacts | An artifact jar becomes a plugin for every session
Hide
Type "ch72/server.sh bare >/dev/null; clear" Enter
Wait@180s
Show
Type "# a server with the plugin configured, but without its jar" Enter
Sleep 500ms
Type "python ch72/artifacts.py 2>/dev/null | cut -c1-92" Enter
Wait@180s
Sleep 10s
Hide
Type "ch72/server.sh stop; clear" Enter
Wait@60s

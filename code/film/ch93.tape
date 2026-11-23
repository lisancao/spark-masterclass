# Spark Masterclass, Chapter 93: MLlib beyond regression. Storyboard: notes/v3-ml-testing.md.
#   film render code/film/ch93.tape --format chapter   (start ch93/server.sh first)
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 BOOK_DATA=$HOME/spark-masterclass/data SPARK_UI_PORT=4158 SPARK_LOCAL_IP=127.0.0.1 && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: rules | Trips as baskets: FPGrowth association rules
Type "# each trip is a basket of items: zones, payment, night, tolls. FPGrowth finds the rules" Enter
Sleep 500ms
Type "python ch93/patterns.py 2>/dev/null | sed -n '/association rules/,/only showing/p' | cut -c1-92" Enter
Wait@900s
Sleep 10s

# beat: connect | Every algorithm of the chapter, Classic and Connect
Type "clear" Enter
Type "# fit each algorithm of this chapter in a Classic session and over Spark Connect" Enter
Sleep 500ms
Type "python ch93/connect_matrix.py 2>/dev/null | cut -c1-80" Enter
Wait@900s
Sleep 10s

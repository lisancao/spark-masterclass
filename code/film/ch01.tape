# Spark Masterclass, Chapter 1: from MapReduce to Spark. Storyboard: storyboard-background.md.
#   film render code/film/ch01.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 BOOK_DATA=$HOME/spark-masterclass/data SPARK_UI_PORT=4141 SPARK_LOCAL_IP=127.0.0.1 && setopt interactivecomments && alias nvim='nvim -u $FILM_KIT/nvim-film.lua' && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: code | The two styles of the same iterative job
Hide
Type "nvim +/'^def mapreduce' ch01/iterative.py" Enter
Sleep 2.5s
Type ":noh" Enter
Type "zt" Ctrl+L
Show
Sleep 9s
Hide
Type "/^def rdd" Enter
Type ":noh" Enter
Type "zt" Ctrl+L
Show
Sleep 7s
Hide
Type ":qa!" Enter
Type "clear" Enter
Sleep 500ms

# beat: passes | Ten passes each: disk every time, or memory after the first
Show
Type "# the same job, ten passes: MapReduce style, then cache()" Enter
Sleep 500ms
Type "python ch01/iterative.py 10 2>/dev/null | sed -n '1,19p'" Enter
Hide
Wait@180s
Show
Sleep 12s

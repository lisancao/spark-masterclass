# Spark Masterclass, Chapter 34. Clips for the book page.
#   film render code/film/ch34.tape --format chapter
Set TypingSpeed 35ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4023 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments && alias nvim='nvim -u $FILM_KIT/nvim-film.lua' && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: ch34-unnest | New in 4.3: UNNEST, WITH ORDINALITY and LEFT JOIN LATERAL
Hide
Type "clear" Enter
Show
Type "# UNNEST (4.3): several arrays in parallel, and the plan of LEFT JOIN LATERAL" Enter
Sleep 500ms
Type "SPARK_UI_PORT=4152 python ch34/unnest.py 2>/dev/null | sed -n '/several arrays/,/NULL array/p;/the plans/,/LATERAL VIEW/p' | cut -c1-92" Enter
Wait@300s
Sleep 10s

# Spark Masterclass, Chapter 28. Clips for the book page.
#   film render code/film/ch28.tape --format chapter
Set TypingSpeed 35ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4023 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments && alias nvim='nvim -u $FILM_KIT/nvim-film.lua' && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: ch28-lambda | A lambda is traced once
Hide
Type "nvim +/'^def double' ch28/lambdas.py" Enter
Sleep 2.5s
Type ":noh" Enter
Type "zt" Ctrl+L
Show
Sleep 7s
Hide
Type ":qa!" Enter
Type "clear" Enter
Sleep 500ms
Show
Type "# the Python lambda runs once, on the driver, with a placeholder" Enter
Sleep 500ms
Type "python ch28/lambdas.py 2>/dev/null | sed -n '/the lambda runs once/,/errors that show/p' | cut -c1-92" Enter
Wait@300s
Sleep 10s

# beat: ch28-codegen | Higher-order functions in codegen
Hide
Type "nvim +/'which higher-order functions take' ch28/lambdas.py" Enter
Sleep 2.5s
Type ":noh" Enter
Type "zt" Ctrl+L
Show
Sleep 7s
Hide
Type ":qa!" Enter
Type "clear" Enter
Sleep 500ms
Show
Type "# new in 4.3: which higher-order functions are compiled" Enter
Sleep 500ms
Type "python ch28/lambdas.py 2>/dev/null | sed -n '/which higher-order/,/nested schema/p' | cut -c1-92" Enter
Wait@300s
Sleep 10s

# beat: ch28-unnest | New in 4.3: UNNEST, WITH ORDINALITY and LEFT JOIN LATERAL
Hide
Type "clear" Enter
Show
Type "# UNNEST (4.3): several arrays in parallel, and the plan of LEFT JOIN LATERAL" Enter
Sleep 500ms
Type "SPARK_UI_PORT=4152 python ch28/unnest.py 2>/dev/null | sed -n '/several arrays/,/NULL array/p;/the plans/,/LATERAL VIEW/p' | cut -c1-92" Enter
Wait@300s
Sleep 10s

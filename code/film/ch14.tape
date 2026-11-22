# Spark Masterclass, Chapter 14. Clips for the book page.
#   flock $HOME/spark-masterclass/data/.bench.lock film render code/film/ch14.tape --format chapter
Set TypingSpeed 35ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4023 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments && alias nvim='nvim -u $FILM_KIT/nvim-film.lua' && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: ch14-lambda | A lambda is traced once
Hide
Type "nvim +/'^def double' ch14/lambdas.py" Enter
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
Type "python ch14/lambdas.py 2>/dev/null | sed -n '/the lambda runs once/,/errors that show/p' | cut -c1-92" Enter
Wait@300s
Sleep 10s

# beat: ch14-codegen | Higher-order functions in codegen
Hide
Type "nvim +/'which higher-order functions take' ch14/lambdas.py" Enter
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
Type "python ch14/lambdas.py 2>/dev/null | sed -n '/which higher-order/,/nested schema/p' | cut -c1-92" Enter
Wait@300s
Sleep 10s

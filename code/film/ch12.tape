# Spark Masterclass, Chapter 12. Clips for the book page.
#   flock $HOME/spark-masterclass/data/.bench.lock film render code/film/ch12.tape --format chapter
Set TypingSpeed 35ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4023 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments && alias nvim='nvim -u $FILM_KIT/nvim-film.lua' && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: ch12-quarantine | A quarantine that balances
Hide
Type "nvim +/'quarantine: parsed' ch12/logs.py" Enter
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
Type "# every input line is parsed or rejected, and the counts agree" Enter
Sleep 500ms
Type "python ch12/logs.py 2>/dev/null | sed -n '/quarantine: parsed/,/sessions from/p' | cut -c1-92" Enter
Wait@300s
Sleep 10s

# beat: ch12-decimal | Decimal commas and locales
Hide
Type "nvim +/'decimal commas' ch12/csv_options.py" Enter
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
Type "# a European amount, read as DECIMAL and DOUBLE with two locales" Enter
Sleep 500ms
Type "python ch12/csv_options.py 2>/dev/null | sed -n '/decimal commas/,/== encodings/p' | cut -c1-92" Enter
Wait@300s
Sleep 10s

# beat: ch12-modes | Parse modes
Hide
Type "nvim +/'parse modes and the corrupt' ch12/csv_drift.py" Enter
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
Type "# the corrupt record column, and DROPMALFORMED gives three answers" Enter
Sleep 500ms
Type "python ch12/csv_drift.py 2>/dev/null | sed -n '/count the corrupt/,/a folder whose/p' | cut -c1-92" Enter
Wait@300s
Sleep 10s

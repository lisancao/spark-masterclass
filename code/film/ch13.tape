# Spark Masterclass, Chapter 13. Clips for the book page.
#   flock $HOME/spark-masterclass/data/.bench.lock film render code/film/ch13.tape --format chapter
Set TypingSpeed 35ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4023 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments && alias nvim='nvim -u $FILM_KIT/nvim-film.lua' && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: ch13-quarantine | Parse modes and the books
Hide
Type "nvim +/'the books must balance' ch13/drift.py" Enter
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
Type "# clean + quarantine = read, file by file: no record is lost" Enter
Sleep 500ms
Type "python ch13/drift.py 2>/dev/null | sed -n '/the books must balance/,/arrays to rows/p' | cut -c1-92" Enter
Wait@300s
Sleep 10s

# beat: ch13-json-table | JSON_TABLE in SQL
Hide
Type "nvim +/'JSON_TABLE(' ch13/functions.py" Enter
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
Type "# new in 4.3: JSON_TABLE turns JSON text into typed rows" Enter
Sleep 500ms
Type "python ch13/functions.py 2>/dev/null | sed -n '/new in 4.3: JSON_TABLE ==/,/JSON_TABLE: missing/p' | head -20" Enter
Wait@300s
Sleep 10s

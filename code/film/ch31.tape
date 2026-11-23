# Spark Masterclass, Chapter 31. Clips for the book page.
#   film render code/film/ch31.tape --format chapter
Set TypingSpeed 35ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4023 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments && alias nvim='nvim -u $FILM_KIT/nvim-film.lua' && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: ch31-forget | A delete leaves the bytes
Hide
Type "nvim +/'forget one file' ch31/blobs.py" Enter
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
Type "# delete one e-mail: the rows go at once, the bytes go later" Enter
Sleep 500ms
Type "python ch31/blobs.py 2>/dev/null | sed -n '/forget one file/,/the blob in the store/p' | cut -c1-92" Enter
Wait@300s
Sleep 10s

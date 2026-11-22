# Spark Masterclass, Chapter 25. Clips for the book page.
#   film render code/film/ch25.tape --format chapter
Set TypingSpeed 35ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4023 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments && alias nvim='nvim -u $FILM_KIT/nvim-film.lua' && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: ch25-bytes | Metadata without content, then magic bytes
Hide
Type "nvim +/'content is read only when' ch25/bytes.py" Enter
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
Type "# a metadata query reads no content; a content read checks maxLength" Enter
Sleep 500ms
Type "python ch25/bytes.py 2>/dev/null | sed -n '/content is read only/,/magic bytes, not/p' | cut -c1-92" Enter
Wait@300s
Sleep 10s

# beat: ch25-archives | The archive reader, off and on
Hide
Type "nvim +/'archive.reader.enabled' ch25/archives.py" Enter
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
Type "# new in 4.3: read the files inside a zip, entry by entry" Enter
Sleep 500ms
Type "python ch25/archives.py 2>/dev/null | sed -n '/before 4.3/,/archivePathFilter picks/p' | cut -c1-92" Enter
Wait@300s
Sleep 10s

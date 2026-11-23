# Spark Masterclass, Chapter 90: the VARIANT binary format. Storyboard: storyboard-part10.md.
#   film render code/film/ch90.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4031 && setopt interactivecomments && alias nvim='nvim -u $FILM_KIT/nvim-film.lua' && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: layout | One event, byte by byte
Type "# one event as a VARIANT: the value bytes, field by field" Enter
Sleep 500ms
Type "python ch90/layout.py 2>/dev/null | sed -n '/^--- value/,/user_id  at/p' | cut -c1-92" Enter
Wait@180s
Sleep 12s

# beat: handmade | Unsorted field ids, made by hand
Hide
Type "clear" Enter
Show
Type "# bytes made in Python: an object whose field ids break the sort rule" Enter
Sleep 500ms
Type "python ch90/handmade.py 2>/dev/null | sed -n '/^hand-made/,/40 fields/p'" Enter
Wait@180s
Sleep 12s

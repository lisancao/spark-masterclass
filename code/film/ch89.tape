# Spark Masterclass, Chapter 89: the Python worker protocol. Storyboard: storyboard-part10.md.
#   film render code/film/ch89.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4031 && setopt interactivecomments && alias nvim='nvim -u $FILM_KIT/nvim-film.lua' && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: tap | The bytes out of a worker
Type "# tap a worker: the response of a pickled UDF, byte by byte" Enter
Sleep 500ms
Type "python ch89/tap.py 2>/dev/null | sed -n '/^response: 102/,/accumulators 0/p' | cut -c1-92" Enter
Wait@180s
Sleep 12s

# beat: failures | Six ways a UDF fails
Hide
Type "clear" Enter
Show
Type "# six UDFs that fail: what does the driver see for each?" Enter
Sleep 500ms
Type "ch89/run_failures.sh | sed -n '1,/^--- the traceback/p' | head -n -2 | cut -c1-92" Enter
Wait@240s
Sleep 12s

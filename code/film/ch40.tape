# Spark Masterclass, Chapter 40. Storyboard: storyboard-part05.md.
#   flock ~/spark-masterclass/data/.bench.lock film render code/film/ch40.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4025 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments && alias nvim='nvim -u $FILM_KIT/nvim-film.lua' && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show


# beat: dead-letters | Dead letters, safe for a replay
Type "# four drops through try_ parsing; bad lines go to dead letters with a reason" Enter
Sleep 500ms
Type "python ch40/m3_dead_letters.py 2>/dev/null | head -17" Enter
Wait@240s
Sleep 12s

# beat: future | One event from tomorrow
Type "# the naive query: one event dated the next day moves the watermark a full day" Enter
Sleep 500ms
Type "python ch40/m4_watermark.py naive 2>/dev/null | head -9" Enter
Wait@240s
Sleep 12s

# Spark Masterclass, Chapter 41. Storyboard: storyboard-part05.md.
#   flock ~/spark-masterclass/data/.bench.lock film render code/film/ch41.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4025 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments && alias nvim='nvim -u $FILM_KIT/nvim-film.lua' && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show


# beat: sessions | Sessions closed by gaps and by timers
Type "# sessions per user: closed by a 30-minute gap or by an event-time timer" Enter
Sleep 500ms
Type "python ch41/t2_sessions.py 2>/dev/null | head -21" Enter
Wait@240s
Sleep 12s

# beat: clock | Processing-time timers and TTL
Type "# a timer 5 s after each click; the last page kept 8 s (TTL)" Enter
Sleep 500ms
Type "python ch41/t3_time.py 2>/dev/null | head -11" Enter
Wait@240s
Sleep 10s

# beat: legacy | applyInPandasWithState, then its open sessions move to transformWithState
Type "# the old API keeps one state value per user; then its open sessions move to TWS" Enter
Sleep 500ms
Type "python ch41/t6_legacy.py 2>/dev/null | sed -n '1,11p;42,52p'" Enter
Wait@300s
Sleep 12s

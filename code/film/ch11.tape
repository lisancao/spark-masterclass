# Spark Masterclass, Chapter 11: Joins. VO lines: notes/part02.md.
#   film render code/film/ch11.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4122 BOOK_NO_LOCK=1 PYTHONUNBUFFERED=1 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: types | Join types and the default strategy
Hide
Type "clear" Enter
Show
Type "# join types on a tiny table, then trips x zones: which strategy?" Enter
Sleep 500ms
Type "python ch11/strategies.py 2>/dev/null | sed -n '/== 2/,/== 3/{/== 3/q;p}' | cut -c1-92" Enter
Wait@300s
Sleep 10s

# beat: asof | ASOF JOIN, new in 4.3
Hide
Type "clear" Enter
Show
Type "# new in 4.3: ASOF JOIN, the last drop-off before each pickup" Enter
Sleep 500ms
Type "python ch11/asof.py 2>/dev/null | sed -n '/== 2/,/== 3/{/== 3/q;p}' | cut -c1-92" Enter
Wait@300s
Sleep 10s

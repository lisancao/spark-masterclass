# Spark Masterclass, Chapter 50: Python data sources IV. Storyboard: storyboard-part09a.md.
#   film render code/film/ch50.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4029 BOOK_DATA=$HOME/spark-masterclass/data CH51_VENVS=/tmp/claude-1000/-home-lnc-repos/ba9be504-7f98-4ba6-9537-bf1ce27aaf62/scratchpad/part09a/venvs && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show


# beat: commit | A manifest commit, and a failed job
Hide
Type "clear" Enter
Show
Type "# append, append, overwrite, then a write that fails" Enter
Sleep 500ms
Type "python ch50/writers.py 2>/dev/null | sed -n '/^driver/,/^  JsonLogWriter.abort/p' | cut -c1-92" Enter
Wait@300s
Sleep 12s

# beat: stream | A stream, its checkpoint, a restart and a replay
Hide
Type "clear" Enter
Show
Type "# a stream reader over a folder, and a sink that skips replays" Enter
Sleep 500ms
Type "python ch50/streaming.py 2>/dev/null | sed -n '1,/^sink:/p' | cut -c1-92" Enter
Wait@300s
Sleep 12s

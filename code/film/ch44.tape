# Spark Masterclass, Chapter 44: Spark Connect in production. Storyboard: notes/part08.md.
#   film render code/film/ch44.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 BOOK_DATA=$HOME/spark-masterclass/data SPARK_UI_PORT=4028 PATH=$FILM_DEMO/.venv/bin:$PATH && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: isolation | Two sessions on one server: what is isolated, what is shared
Type "# session A sets state; what does session B on the same server see?" Enter
Sleep 500ms
Type "python ch44/isolation.py 2>/dev/null" Enter
Wait@240s
Sleep 12s

# beat: lifecycle | Idle timeout, release, restart, interrupt
Hide
Type "clear" Enter
Show
Type "# how a session ends: timeout, release, server restart, interruptTag" Enter
Sleep 500ms
Type "python ch44/lifecycle.py 2>/dev/null | cut -c1-94" Enter
Wait@240s
Sleep 12s

# beat: feather | Single-task execution in one session of a shared server
Hide
Type "clear" Enter
Show
Type "# new in 4.3: one session turns on single-task execution" Enter
Sleep 500ms
Type "python ch44/feather_server.py 2>/dev/null" Enter
Wait@240s
Sleep 12s

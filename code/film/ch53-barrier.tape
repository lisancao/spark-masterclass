# Spark Masterclass, Chapter 53 (Step 12): barrier execution mode. Storyboard: notes/v3-ml-testing.md.
#   film render code/film/ch53-barrier.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 BOOK_DATA=$HOME/spark-masterclass/data SPARK_UI_PORT=4158 SPARK_LOCAL_IP=127.0.0.1 && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: barrier | All tasks start together, wait for each other, and fail together
Type "# barrier mode: every task waits for all the others; too few slots and the job fails" Enter
Sleep 500ms
Type "python ch53/barrier.py 2>/dev/null | cut -c1-92" Enter
Wait@600s
Sleep 10s

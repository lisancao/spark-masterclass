# Spark Masterclass, Chapter 42: partitions, caching, memory and spill (tuning steps).
#   film render code/film/ch42.tape --format chapter --demo code
Set TypingSpeed 40ms
Set WaitPattern /❯$/

Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4154 BOOK_DATA=$HOME/spark-masterclass/data BOOK_NO_LOCK=1 && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: locality | Delay scheduling on two executors: the default wait, and no wait
Type "# two executors: where does each 1 s task run, and when?" Enter
Sleep 500ms
Type "python ch42/locality.py 2>/dev/null | sed -n '1,22p' | cut -c1-94" Enter
Wait@600s
Sleep 12s

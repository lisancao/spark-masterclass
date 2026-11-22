# Spark Masterclass, Chapter 18: reading query plans. 
#   film render code/film/ch18.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4054 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: rules | Which optimizer rules changed the plan
Type "# 272 rules run on one query; which of them changed the plan?" Enter
Sleep 500ms
Type "python ch18/optimizer.py 2>/dev/null | sed -n '/rules ran/,$p'" Enter
Wait@120s
Sleep 9s

# beat: join | AQE changes a sort-merge join to a broadcast join
Hide
Type "clear" Enter
Show
Type "# the same join twice: AQE keeps sort-merge, then switches to broadcast" Enter
Sleep 500ms
Type "python ch18/aqe.py joinbrief 2>/dev/null"
Wait@180s
Sleep 12s

# beat: skew | AQE splits the skewed pickup zones
Hide
Type "clear" Enter
Show
Type "# a sort-merge join on pickup zone: the busiest zones are 300x the median" Enter
Sleep 500ms
Type "python ch18/aqe.py skew 2>/dev/null | sed -n '/^rows/,$p' | cut -c1-94" Enter
Wait@180s
Sleep 10s

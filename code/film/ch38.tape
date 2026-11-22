# Spark Masterclass, Chapter 38: acceleration. Storyboard: storyboard.md.
#   flock ~/spark-masterclass/data/.bench.lock film render code/film/ch38.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4062 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments && alias python=.venv/bin/python && alias py41=$BOOK_DATA/ch38/venv-4.1.1/bin/python py42=$BOOK_DATA/ch38/venv-4.2.0/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: blocked | The newest engine releases on Spark 4.3.0
Type "# Spark 4.3.0 with the newest RAPIDS and Gluten jars: does each one start?" Enter
Sleep 500ms
Type "python ch38/blocked_43.py --one rapids 2>/dev/null | cut -c1-170 | fold -w 90" Enter
Wait@180s
Sleep 4s
Type "python ch38/blocked_43.py --one gluten 2>/dev/null | fold -w 90" Enter
Wait@180s
Sleep 8s

# beat: fallback | Gluten with Spark's defaults, then with two settings changed
Hide
Type "clear" Enter
Show
Type "# Gluten 1.7.0 on Spark 4.1.1, Spark defaults (ANSI on, TIMESTAMP_NTZ)" Enter
Sleep 500ms
Type "py41 ch38/plans.py gluten --defaults hourly 2>/dev/null | grep '^hourly'" Enter
Wait@240s
Sleep 3s
Type "# the same query with ANSI off and pickup times read as TIMESTAMP" Enter
Sleep 500ms
Type "py41 ch38/plans.py gluten hourly 2>/dev/null | grep '^hourly'" Enter
Wait@240s
Sleep 10s

# beat: gpu | The same query on the GPU
Hide
Type "clear" Enter
Show
Type "# RAPIDS 26.10.0 (cuDF, CUDA 13) on Spark 4.2.0, RTX 5070 Ti" Enter
Sleep 500ms
Type "py42 ch38/plans.py rapids hourly 2>/dev/null | tail -n +2" Enter
Wait@240s
Sleep 10s

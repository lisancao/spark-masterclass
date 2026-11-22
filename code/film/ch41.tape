# Spark Masterclass, Chapter 41: determinism, retries and fault injection. Storyboard: notes/part08.md.
#   flock $HOME/spark-masterclass/data/.bench.lock film render code/film/ch41.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 BOOK_DATA=$HOME/spark-masterclass/data SPARK_UI_PORT=4028 && setopt interactivecomments && alias nvim='nvim -u $FILM_KIT/nvim-film.lua' && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: attempts | local[4] never retries, local[4,2] does
Type "# a UDF that fails once, on the first attempt of partition 1" Enter
Sleep 500ms
Type "python ch41/attempts.py 2>/dev/null | head -10" Enter
Wait@180s
Sleep 10s

# beat: shuffle | A lost shuffle file, three ways
Hide
Type "clear" Enter
Show
Type "# delete one map output, then run the same DataFrame again" Enter
Sleep 500ms
Type "python ch41/lost_shuffle.py 2>/dev/null | sed -n '/^modulo/,$p' | grep -v 'stage [23]'" Enter
Wait@180s
Sleep 12s

# beat: reattach | A cut connection, and the same operation ID
Hide
Type "ch41/server.sh start --conf spark.connect.execute.reattachable.senderMaxStreamSize=16m >/dev/null; clear" Enter
Wait@180s
Show
Type "# 3.8M rows over Connect, through a proxy that cuts the TCP connection" Enter
Sleep 500ms
Type "python ch41/reattach.py 2>/dev/null" Enter
Wait@180s
Sleep 12s

# beat: fragile | The fragile job and the correct job under chaos
Hide
Type "clear" Enter
Show
Type "# each job: a clean run, then a chaos run with retries and a rerun" Enter
Sleep 500ms
Type "python ch41/fragile_job.py 2>/dev/null" Enter
Wait@180s
Sleep 12s

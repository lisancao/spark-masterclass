# Spark Masterclass, Chapter 73: shipping Python dependencies. Storyboard: notes/part08.md.
#   flock $HOME/spark-masterclass/data/.bench.lock film render code/film/ch73.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 BOOK_DATA=$HOME/spark-masterclass/data SPARK_UI_PORT=4028 PATH=$FILM_DEMO/.venv/bin:$PATH && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "source ch73/film_setup.sh" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: where | What a worker sees, from two folders
Type "# a UDF prints its worker's interpreter, folder and sys.path" Enter
Sleep 500ms
Type "python ch73/where.py 2>/dev/null | short" Enter
Wait@180s
Sleep 4s
Type "# the same script, started in a folder that holds messytools/" Enter
Sleep 500ms
Type "(cd /tmp/pkg && $CODE/.venv/bin/python $CODE/ch73/where.py 2>/dev/null | short | tail -6)" Enter
Wait@180s
Sleep 10s

# beat: includes | A UDF records its include list one time
Hide
Type "clear" Enter
Show
Type "# make a UDF, use it, then addPyFile: which UDF finds the module?" Enter
Sleep 500ms
Type "python ch73/pyfiles.py 2>/dev/null | sed -n '5,11p;11q' | sed 's#<userFiles>/messytools.zip/messytools/__init__.py#zip#g'" Enter
Wait@240s
Sleep 12s

# beat: artifacts | Two Connect sessions, two versions of one module
Hide
Type "ch72/server.sh start >/dev/null; clear" Enter
Wait@180s
Show
Type "# two Connect sessions on one server ship two versions of messytools" Enter
Sleep 500ms
Type "python ch73/artifacts.py 2>/dev/null | cut -c1-92" Enter
Wait@180s
Sleep 12s

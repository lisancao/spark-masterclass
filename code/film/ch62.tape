# Spark Masterclass, Chapter 62: from wrong results to a fix. Storyboard: storyboard-part10.md.
#   film render code/film/ch62.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_SRC=$HOME/repos/spark && setopt interactivecomments && alias nvim='nvim -u $FILM_KIT/nvim-film.lua' && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: repro | One metric view, two answers
Type "# SPARK-60206: the same metric view, queried from two namespaces" Enter
Sleep 500ms
Type "python ch62/repro.py 2>/dev/null" Enter
Wait@180s
Sleep 10s

# beat: fix | The fix, compiled against the 4.3.0 jars
Hide
Type "clear" Enter
Show
Type "# patch two Scala files, compile them against the pyspark jars, run again" Enter
Sleep 500ms
Type "ch62/test_fix.sh 2>/dev/null | grep -v '^$' | cut -c1-92 | head -14" Enter
Wait@300s
Sleep 12s

# Spark Masterclass, Chapter 29. Storyboard: storyboard-part05.md.
#   flock ~/spark-masterclass/data/.bench.lock film render code/film/ch29.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4025 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments && alias nvim='nvim -u $FILM_KIT/nvim-film.lua' && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show


# beat: decode | A delta file, decoded by hand
Type "# a per-user count on the HDFS state store; its delta files decoded with plain Python" Enter
Sleep 500ms
Type "python ch29/c1_hdfs.py 2>/dev/null | sed -n '/decoded/,/state data source/p' | cut -c1-94" Enter
Wait@240s
Sleep 12s

# beat: repartition | Two state partitions become four
Type "# repartition the state of a stopped query from 2 to 4 partitions, then restart it" Enter
Sleep 500ms
Type "python ch29/c3_repartition.py 2>/dev/null | head -14 | cut -c1-94" Enter
Wait@240s
Sleep 12s

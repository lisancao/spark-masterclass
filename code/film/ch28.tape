# Spark Masterclass, Chapter 28. Storyboard: storyboard-part05.md.
#   flock ~/spark-masterclass/data/.bench.lock film render code/film/ch28.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4025 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments && alias nvim='nvim -u $FILM_KIT/nvim-film.lua' && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show


# beat: rules | What Real-time Mode accepts
Type "# each probe starts a query with trigger(realTime='5 seconds')" Enter
Sleep 500ms
Type "python ch28/r1_rules.py 2>/dev/null | grep -E '^(Each|The trigger|  )' | sed -E 's/STREAMING_REAL_TIME_MODE/RTM/; s/  [(]from the running query[)]/ (late)/'" Enter
Wait@400s
Sleep 12s

# beat: latency | Latency, micro-batch against Real-time
Type "# 200 records/s through a running count, Kafka to Kafka, both engines" Enter
Sleep 500ms
Type "python ch28/r2_latency.py 200 20 mb_count rtm_count 2>/dev/null | sed -E 's/ +/ /g'" Enter
Wait@400s
Sleep 12s

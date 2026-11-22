# Spark Masterclass, Chapter 24: Structured Streaming fundamentals. Storyboard: storyboard-part05.md.
#   flock ~/spark-masterclass/data/.bench.lock film render code/film/ch24.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4055 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments && alias nvim='nvim -u $FILM_KIT/nvim-film.lua' && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: ticking | A stream that ticks
Type "# a file of trips arrives every 4 s; the query runs a micro-batch every 2 s" Enter
Sleep 500ms
Type "python ch24/s3_ticking.py 2>/dev/null" Enter
Wait@180s
Sleep 8s

# beat: watermark | A watermark that advances over late trips
Type "# count trips by pickup time, 10-minute windows, watermark 15 minutes behind" Enter
Sleep 500ms
Type "python ch24/s6_watermark.py 2>/dev/null | head -16" Enter
Wait@180s
Sleep 12s

# beat: crash | The driver dies inside batch 3
Type "# count trips per zone; the driver dies after the sink writes batch 3" Enter
Sleep 500ms
Type "python ch24/s7_restart.py crash 2>/dev/null" Enter
Wait@180s
Sleep 4s
Type "ls ../data/work/ch24/s7-restart/checkpoint/{offsets,commits}" Enter
Sleep 7s

# beat: resume | Restart from the checkpoint
Type "# same checkpoint, new process: batch 3 runs again with the same input" Enter
Sleep 500ms
Type "python ch24/s7_restart.py resume 2>/dev/null" Enter
Wait@180s
Sleep 10s

# Spark Masterclass, Chapter 64: writing Spark pipelines with agents. Storyboard: notes/part08.md.
#   film render code/film/ch64.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 BOOK_DATA=$HOME/spark-masterclass/data SPARK_UI_PORT=4028 PATH=$FILM_DEMO/.venv/bin:$PATH && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show
# beat: dryrun | The dry-run refuses three agent errors before any data
Type "# five versions of the gold flow: which ones does the dry-run refuse?" Enter
Sleep 500ms
Type "ch64/dryrun.sh 2>/dev/null | grep --line-buffered -v 'AnalysisException: *$\|raise PySparkException' | cut -c1-94" Enter
Wait@400s
Sleep 12s

# beat: metrics | By-hand SQL against MEASURE() on a metric view
Hide
Type "clear" Enter
Show
Type "# the agent's by-hand SQL against MEASURE() on a metric view" Enter
Sleep 500ms
Type "python ch64/metric_view.py 2>/dev/null | grep -v DESCRIBE" Enter
Wait@240s
Sleep 12s

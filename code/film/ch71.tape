# Spark Masterclass, Chapter 71: what Spark redacts, and what local disk encryption writes.
#   film render code/film/ch71.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 BOOK_DATA=$HOME/spark-masterclass/data SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4151 && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: redaction | Secrets in the UI, the event log, SQL and the application
Type "# three secrets in the configuration: what the UI, the event log and the app see" Enter
Sleep 300ms
Type "python ch71/redaction.py 2>/dev/null | cut -c1-92" Enter
Wait@120s
Sleep 10s

# beat: disk | The first bytes of a shuffle file and a cached block, encryption off and on
Hide
Type "clear" Enter
Show
Type "# spark.io.encryption.enabled: the first bytes of files in spark.local.dir" Enter
Sleep 300ms
Type "for v in false true; do python ch71/io_encryption.py $v 2>/dev/null | cut -c1-92; done" Enter
Wait@200s
Sleep 10s

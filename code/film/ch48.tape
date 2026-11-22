# Spark Masterclass, Chapter 48: Python data sources II. Storyboard: storyboard-part09a.md.
#   film render code/film/ch48.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4029 BOOK_DATA=$HOME/spark-masterclass/data CH51_VENVS=/tmp/claude-1000/-home-lnc-repos/ba9be504-7f98-4ba6-9537-bf1ce27aaf62/scratchpad/part09a/venvs && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show


# beat: requests | Requests for each step
Hide
Type "clear" Enter
Show
Type "# count the requests that each step sends to the tickets API" Enter
Sleep 500ms
Type "python ch48/api_source.py 2>/dev/null | grep -E '^(load|explain|count|groupBy|cache|two more|page_size|pages_per)'" Enter
Wait@300s
Sleep 12s

# beat: ranges | Key ranges: stride and quantile
Hide
Type "clear" Enter
Show
Type "# JDBC ranges are equal in key space, quantile ranges are equal in rows" Enter
Sleep 500ms
Type "python ch48/database_source.py 2>/dev/null | sed -n '/^JDBC/,/^quantile split/p'" Enter
Wait@300s
Sleep 12s

# beat: timezone | The time zone of the worker
Hide
Type "clear" Enter
Show
Type "# the same naive datetime under three worker time zones" Enter
Sleep 500ms
Type "for tz in UTC America/Los_Angeles Asia/Ho_Chi_Minh; do TZ=$tz python ch48/arrow_batches.py --tz 2>/dev/null; done" Enter
Wait@300s
Sleep 12s

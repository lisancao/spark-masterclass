# Spark Masterclass, Chapter 52: Python data sources III. Storyboard: storyboard-part09a.md.
#   film render code/film/ch52.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4029 BOOK_DATA=$HOME/spark-masterclass/data CH51_VENVS=/tmp/claude-1000/-home-lnc-repos/ba9be504-7f98-4ba6-9537-bf1ce27aaf62/scratchpad/part09a/venvs && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show


# beat: offered | What Spark offers to pushFilters()
Hide
Type "clear" Enter
Show
Type "# which filter objects does Spark offer for each WHERE condition?" Enter
Sleep 500ms
Type "python ch52/what_arrives.py 2>/dev/null | sed -n '/^WHERE/,/^id > 1 AND/p' | cut -c1-92" Enter
Wait@300s
Sleep 12s

# beat: opened | Rows, files opened, and Spark's Filter
Hide
Type "clear" Enter
Show
Type "# the audit counts the files that the tasks opened" Enter
Sleep 500ms
Type "python ch52/files_pushdown.py 2>/dev/null | sed -n '1,/^lower/p' | cut -c1-92" Enter
Wait@300s
Sleep 12s

# beat: shared | The shared planned read
Hide
Type "clear" Enter
Show
Type "# 4.3.0: one DataFrame, a pushed filter first, then other queries" Enter
Sleep 500ms
Type "python ch52/files_pushdown.py 2>/dev/null | sed -n '/^One DataFrame/,$p'" Enter
Wait@300s
Sleep 12s

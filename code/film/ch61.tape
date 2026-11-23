# Spark Masterclass, Chapter 61: Auto CDC. Storyboard: notes/part06.md (film section).
#   film render code/film/ch61.tape --format chapter (after ch61/drops.sh)
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 BOOK_DATA=${BOOK_DATA:-$HOME/spark-masterclass/data} SPARK_UI_PORT=4026 W=${BOOK_DATA:-$HOME/spark-masterclass/data}/work/part06 && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: history | Toronto, Ottawa, Hanoi: the late event in the history
Type "# three change files applied: the SCD Type 2 history, read from Iceberg" Enter
Sleep 500ms
Type "python ch61/show_targets.py 2>/dev/null | sed -n '/customers_history/,/^$/p'" Enter
Wait@120s
Sleep 12s

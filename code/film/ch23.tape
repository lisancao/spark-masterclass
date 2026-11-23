# Spark Masterclass, Chapter 23: metric views. Storyboard: notes/part06.md (film section).
#   film render code/film/ch23.tape --format chapter (after ch23/define.py)
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 BOOK_DATA=${BOOK_DATA:-$HOME/spark-masterclass/data} SPARK_UI_PORT=4026 W=${BOOK_DATA:-$HOME/spark-masterclass/data}/work/part06 && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: measure | The average of ratios, and MEASURE()
Type "# one ratio, two answers: by hand, and from the metric view" Enter
Sleep 500ms
Type "python ch23/film.py 2>/dev/null" Enter
Wait@180s
Sleep 12s

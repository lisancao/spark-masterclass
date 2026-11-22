# Spark Masterclass, Chapter 57: sketches. Storyboard: notes/part07.md.
#   film render code/film/ch57.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 BOOK_DATA=$HOME/spark-masterclass/data SPARK_UI_PORT=4227 SPARK_LOCAL_IP=127.0.0.1 && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: rollups | Rollups from stored sketches after the raw events are deleted
Type "# store daily sketches, delete the raw events, answer from the sketches" Enter
Sleep 500ms
Type "python ch57/sketches.py 2>/dev/null | sed -n '/== sketch table/,/rank of the estimate/p;/^   0\./p' | grep -v 'plan of\|schema' | cut -c1-92" Enter
Wait@1200s
Sleep 12s

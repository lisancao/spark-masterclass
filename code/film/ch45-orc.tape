# Spark Masterclass, Chapter 45: ORC and encryption (Steps 9 to 12).
#   film render code/film/ch45-orc.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4154 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: orc | The two ends of an ORC file, and its postscript decoded by hand
Hide
Type "clear" Enter
Show
Type "# ORC: the last byte, the postscript, the footer and one stripe" Enter
Sleep 500ms
Type "python ch45/orc.py dump 2>/dev/null | sed -n '1,14p' | cut -c1-94" Enter
Wait@300s
Sleep 12s

# beat: crypto | Parquet modular encryption: PARE, and a read without the keys
Hide
Type "clear" Enter
Show
Type "# encrypt two columns and the footer, then read without the keys" Enter
Sleep 500ms
Type "python ch45/crypto_rebase.py crypto 2>/dev/null | cut -c1-94" Enter
Wait@300s
Sleep 12s

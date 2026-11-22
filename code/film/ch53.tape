# Spark Masterclass, Chapter 53: Catalyst extensions. Storyboard: storyboard.md.
#   film render code/film/ch53.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4030 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: rule | A rule replaces one Python UDF by name
Type "# the same Python UDF under two names: the rule replaces only is_png" Enter
Sleep 500ms
Type "python ch53/extensions.py 2>/dev/null | sed -n '/== 7\./,/rewrites so far/p' | cut -c1-92" Enter
Wait@180s
Sleep 12s

# beat: transpile | New in 4.3: the transpiler, and what it refuses
Hide
Type "clear" Enter
Show
Type "# new in 4.3: transpilePyUDFs turns simple Python UDFs into Catalyst" Enter
Sleep 500ms
Type "python ch53/transpile.py 2>/dev/null | sed -n '/== 1\./,/== 4\./p' | grep -v '== 4' | cut -c1-92" Enter
Wait@180s
Sleep 12s

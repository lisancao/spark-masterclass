# Spark Masterclass, Chapter 88: how Catalyst plans Python. Storyboard: storyboard-part10.md.
#   film render code/film/ch88.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4031 && setopt interactivecomments && alias nvim='nvim -u $FILM_KIT/nvim-film.lua' && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: twice | One UDF, counted
Type "# a UDF that counts its own calls: how many calls for each query?" Enter
Sleep 500ms
Type "python ch88/twice.py 2>/dev/null | sed -n '1,/count(), UDF/p'" Enter
Wait@180s
Sleep 12s

# beat: transpile | What the 4.3 transpiler lowers
Hide
Type "clear" Enter
Show
Type "# new in 4.3: transpile Python UDFs into native expressions" Enter
Sleep 500ms
Type "python ch88/transpile.py 2>/dev/null | sed -n '/^3. What/,/^4. ANSI/p' | head -n -1" Enter
Wait@180s
Sleep 12s

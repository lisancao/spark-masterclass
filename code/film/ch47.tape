# Spark Masterclass, Chapter 47: Python data sources I. Storyboard: storyboard-part09a.md.
#   film render code/film/ch47.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4059 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments && alias nvim='nvim -u $FILM_KIT/nvim-film.lua' && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: where | Which process runs each method
Type "# a traced Python data source: each method logs its process id" Enter
Sleep 500ms
Type "python ch47/where_it_runs.py 2>/dev/null | sed -n '/^driver/,/^The plan/p'" Enter
Wait@120s
Sleep 12s

# beat: balance | Partitions balanced by bytes
Hide
Type "clear" Enter
Show
Type "# 26 files to 3 tasks: dealt in turn, or largest file first" Enter
Sleep 500ms
Type "python ch47/docs_source.py 2>/dev/null | sed -n '1,3p;/^Bytes per task/,/^$/p'" Enter
Wait@120s
Sleep 12s

# beat: errors | When each error appears
Hide
Type "clear" Enter
Show
Type "# planning errors fail at the first action, read() errors fail in a task" Enter
Sleep 500ms
Type "python ch47/counter.py 2>/dev/null | sed -n '/^schema()/,$p' | grep -v '^    [FA][an]'" Enter
Wait@120s
Sleep 12s

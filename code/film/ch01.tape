# Spark Masterclass, Chapter 1: how Spark runs your code. Storyboard: storyboard.md.
#   film render code/film/ch01.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 && setopt interactivecomments && alias nvim='nvim -u $FILM_KIT/nvim-film.lua' && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: query | The query, before anything runs
Hide
Type "nvim +/'^by_hour' ch01/anatomy.py" Enter
Sleep 2.5s
Type ":noh" Enter
Type "zt" Ctrl+L
Show
Sleep 9s
Hide
Type ":qa!" Enter
Type "clear" Enter
Sleep 500ms

# beat: jobs | One collect(), four jobs
Show
Type "# one collect(): how many jobs, stages and tasks did it become?" Enter
Sleep 500ms
Type "python ch01/anatomy.py 2>/dev/null | sed -n '/rows back/,/^Inside/p'" Enter
Wait@120s
Sleep 10s

# beat: tasks | Sixteen tasks, four row groups
Hide
Type "clear" Enter
Show
Type "python ch01/anatomy.py 2>/dev/null | sed -n '/^Inside/,$p'   # the scan, task by task" Enter
Wait@120s
Sleep 10s

# beat: feather | A small query, with and without single-task execution
Hide
Type "clear" Enter
Show
Type "# new in 4.3: plan a small query as one task, with no shuffles" Enter
Sleep 500ms
Type "python ch01/feather.py 2>/dev/null | head -2" Enter
Wait@120s
Sleep 8s

# Spark Masterclass, Chapter 71: inside a Spark application. Storyboard: storyboard-background.md.
#   film render code/film/ch71.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 BOOK_DATA=$HOME/spark-masterclass/data SPARK_UI_PORT=4141 SPARK_LOCAL_IP=127.0.0.1 && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: tasks | Two executors, two stages, and where each task ran
Type "# a real driver and two executor JVMs: where did each task run?" Enter
Sleep 500ms
Type "python ch71/anatomy.py 2>/dev/null | sed -n '/^executors/,/^shuffle files/p' | head -24" Enter
Hide
Wait@180s
Show
Sleep 12s

# beat: files | The shuffle files on disk, and the index of offsets
Hide
Type "clear" Enter
Show
Type "# the files that the map tasks wrote, and what an .index file holds" Enter
Sleep 500ms
Type "python ch71/anatomy.py 2>/dev/null | sed -n '/^reduce partition/,/^first 32/p'" Enter
Hide
Wait@180s
Show
Sleep 10s

# beat: dynamic | Dynamic allocation, from the event log
Hide
Type "clear" Enter
Show
Type "# start with zero executors, give the scheduler a backlog, then wait" Enter
Sleep 500ms
Type "python ch71/dynamic.py 2>/dev/null" Enter
Hide
Wait@180s
Show
Sleep 10s

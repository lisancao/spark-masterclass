# Spark Masterclass, Chapter 34: end to end. Storyboard: notes/part06.md (film section).
#   film render code/film/ch34.tape --format chapter (after ch34/end_to_end.sh)
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 BOOK_DATA=${BOOK_DATA:-$HOME/spark-masterclass/data} SPARK_UI_PORT=4026 W=${BOOK_DATA:-$HOME/spark-masterclass/data}/work/part06 && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: trace | What the CLI sent to the server
Type "# the dry run, command by command, as the CLI sent it" Enter
Sleep 500ms
Type "grep -E 'create_|start_run|SINK|_feed' ch34/output/trace.txt | cut -c1-92" Enter
Sleep 9s

# beat: server | What the server did in run 2
Hide
Type "clear" Enter
Show
Type "# the server log of run 2: tables first, then flows in topological order" Enter
Sleep 500ms
Type "grep -E 'Materializing|Starting flow' ch34/output/server_log.txt | sed -E 's/.*(table|flow) //' | head -18" Enter
Sleep 10s

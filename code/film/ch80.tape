# Spark Masterclass, Chapter 80: a standalone cluster for real, and a master that dies.
#   needs nothing running: the hidden setup starts the cluster (ch80/standalone.sh up)
#   film render code/film/ch80.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '" Enter
Type "ch80/standalone.sh down; ch80/standalone.sh up; clear" Enter
Wait@120s
Sleep 1s
Show

# beat: submit | A master, two workers, and one client-mode submit
Type "# a standalone master and two workers from the tarball's sbin/" Enter
Sleep 300ms
Type "ch80/standalone.sh status" Enter
Wait@30s
Sleep 2s
Type "ch80/submit.sh client | grep -v Granted | cut -c1-92" Enter
Wait@120s
Sleep 8s

# beat: ha | SIGKILL the master while an application runs
Hide
Type "clear" Enter
Show
Type "# kill the master during a job; FILESYSTEM recovery brings it back" Enter
Sleep 300ms
Type "ch80/ha.sh 2>/dev/null | grep -v -E 'Registered executor|Asking each|Shutting' | cut -c1-92" Enter
Wait@200s
Sleep 10s

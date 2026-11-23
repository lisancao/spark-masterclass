# Spark Masterclass, Chapter 70: a standalone cluster for real, and a master that dies.
#   needs nothing running: the hidden setup starts the cluster (ch70/standalone.sh up)
#   film render code/film/ch70.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '" Enter
Type `scrub() { sed "s/$(hostname)/laptop/g"; }` Enter
Type "ch70/standalone.sh down; ch70/standalone.sh up; clear" Enter
Wait@120s
Sleep 1s
Show

# beat: submit | A master, two workers, and one client-mode submit
Type "# a standalone master and two workers from the tarball's sbin/" Enter
Sleep 300ms
Type "ch70/standalone.sh status" Enter
Wait@30s
Sleep 2s
Type "ch70/submit.sh client | grep -v Granted | scrub | cut -c1-92" Enter
Wait@120s
Sleep 8s

# beat: ha | SIGKILL the master while an application runs
Hide
Type "clear" Enter
Show
Type "# kill the master during a job; FILESYSTEM recovery brings it back" Enter
Sleep 300ms
Type "ch70/ha.sh 2>/dev/null | grep -v -E 'Registered executor|Asking each|Shutting|Spark Command' | cut -c1-92" Enter
Wait@200s
Sleep 10s

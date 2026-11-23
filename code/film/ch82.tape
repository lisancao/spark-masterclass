# Spark Masterclass, Chapter 82: the hadoop-provided build against the official tarball.
#   needs the build from ch82/build.sh; CH82_TGZ points at spark-4.3.0-bin-book.tgz
#   film render code/film/ch82.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

Hide
Type "cd $FILM_DEMO && export BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: inspect | The build next to the official tarball, then a run with and without Hadoop
Type "# the tgz that make-distribution.sh packed, next to the official 4.3.0 tarball" Enter
Sleep 300ms
Type "ch82/inspect.sh $CH82_TGZ 2>/dev/null | cut -c1-92 | grep -v '^    [a-z]'" Enter
Wait@200s
Sleep 10s

# Spark Masterclass, Chapter 28: building an application on a Connect client.
#   SCRATCH=<build dir> film render code/film/ch28.tape --format chapter
# Needs: ch27/server.sh start and ch28/run.sh build.
Set TypingSpeed 35ms
Set WaitPattern /❯$/

Hide
Type "cd $FILM_DEMO && setopt interactivecomments" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: heavy | Six months aggregated by Spark, back as Arrow, into Polars
Type "# the app: Spark does six months of trips; the answer comes back as Arrow" Enter
Sleep 400ms
Type "ch28/run.sh show | sed -n '/== 1/,/^schema/p'" Enter
Wait@60s
Sleep 6s

# beat: local | Follow-up questions in Polars and DuckDB, no round trip
Hide
Type "clear" Enter
Show
Type "# follow-up questions: Polars and DuckDB on the same Arrow data" Enter
Sleep 400ms
Type "ch28/run.sh show | sed -n '/== 4/,/question 1/p;/busiest/,/^$/p' | grep -v '^shape\|---\|^==\|^$'" Enter
Wait@60s
Sleep 8s

# beat: retry | The server is down when the app starts; retries carry it through
Hide
Type "clear" Enter
Show
Type "# the server is down when the app starts; it comes back after 8 s" Enter
Sleep 400ms
Type "ch28/resilience.sh wait" Enter
Wait@120s
Sleep 6s

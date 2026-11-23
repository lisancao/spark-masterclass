# Spark Masterclass, Chapter 23: writing a Spark Connect client.
#   film render code/film/ch23.tape --format chapter
# Needs: ch21/server.sh start (the last beat restarts it with small streams, then normally).
Set TypingSpeed 35ms
Set WaitPattern /❯$/

Hide
Type "cd $FILM_DEMO && setopt interactivecomments" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: bytes | The smallest ExecutePlanRequest, byte by byte
Type "# no pyspark: build an ExecutePlanRequest and read its 84 bytes" Enter
Sleep 400ms
Type "ch23/step.sh step1_bytes" Enter
Wait@60s
Sleep 7s

# beat: stream | Send it, and read the response stream
Hide
Type "clear" Enter
Show
Type "# one gRPC call: ExecutePlan, then a stream of responses with Arrow batches" Enter
Sleep 400ms
Type "ch23/step.sh step2_execute" Enter
Wait@60s
Sleep 7s

# beat: reattach | The server ends the stream early; the client reattaches
Hide
Type "ch21/server.sh small-streams >/dev/null; until nc -z localhost 15106 2>/dev/null; do sleep 0.3; done; clear" Enter
Wait@120s
Show
Type "# the server ends each stream after 1 MiB: ReattachExecute until ResultComplete" Enter
Sleep 400ms
Type "ch23/step.sh step6_reattach | sed -n '1,3p;14,$p'" Enter
Wait@60s
Sleep 6s
Hide
Type "ch21/server.sh start >/dev/null" Enter
Wait@120s

# Spark Masterclass, Chapter 27: Spark Connect clients beyond Python.
#   SCRATCH=<build dir> film render code/film/ch27.tape --format chapter
# Needs: ch27/server.sh start, and ch27/run_probes.sh once (it builds the Rust, Go and Swift programs).
Set TypingSpeed 35ms
Set WaitPattern /❯$/

Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 && setopt interactivecomments" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: rust | The taxi query from the Rust client
Type "# Rust: the client builds the plan; the 4.3.0 server runs it" Enter
Sleep 400ms
Type "sed -n '16,23p' ch27/rust/src/main.rs" Enter
Sleep 2500ms
Type "ch27/client.sh rust taxi" Enter
Wait@60s
Sleep 6s

# beat: swift | The same query from the Swift client
Hide
Type "clear" Enter
Show
Type "# Swift 0.8.0: the same plan, with Swift operators" Enter
Sleep 400ms
Type "sed -n '11,18p' ch27/swift/Sources/Taxi/main.swift" Enter
Sleep 2500ms
Type "ch27/client.sh swift taxi" Enter
Wait@60s
Sleep 6s

# beat: probe | The capability probe from the Go client
Hide
Type "clear" Enter
Show
Type "# Go v0.1.0: the same checks; zip has no API in this client" Enter
Sleep 400ms
Type "ch27/client.sh go probe" Enter
Wait@60s
Sleep 7s

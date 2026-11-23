# Spark Masterclass, Chapter 92: Datasets and Encoders in Scala
#   film render code/film/ch92.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 BOOK_DATA=$HOME/spark-masterclass/data SPARK_UI_PORT=4157 SPARK_LOCAL_IP=127.0.0.1 PYTHONHASHSEED=0 && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show


# beat: encoders | Encoders and as[T]
Hide
Type "clear" Enter
Show
Type "# an Encoder is a schema plus a converter; as[T] checks the columns" Enter
Sleep 500ms
Type "uv run spark-shell --master 'local[*]' --conf spark.ui.port=4157 -I ch92/Datasets.scala 2>/dev/null > ../data/work/ch92.txt; sed -n '/== 1/,/== 3/p' ../data/work/ch92.txt | cut -c1-94" Enter
Hide
Wait@600s
Show
Sleep 12s

# beat: plans | Typed and untyped plans, measured
Hide
Type "clear" Enter
Show
Type "# the typed plan converts each row to a Trip and back; the untyped plan does not" Enter
Sleep 500ms
Type "sed -n '/== Physical/,/== 6/p' ../data/work/ch92.txt | sed 's/[$]line[0-9]*[.][$]read[$][$]iw[$]//g' | cut -c1-94" Enter
Hide
Wait@600s
Show
Sleep 12s

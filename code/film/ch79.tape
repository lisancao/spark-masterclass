# Spark Masterclass, Chapter 79: configuring a pipeline.
#   film render code/film/ch79.tape --format chapter
Set TypingSpeed 35ms
Set WaitPattern /❯ ?$/

Hide
Type "bash --norc" Enter
Sleep 500ms
Type "export BOOK_DATA=${BOOK_DATA:-$HOME/spark-masterclass/data} CONNECT_PORT=15104 SPARK_UI_PORT=4144; . $FILM_DEMO/film/sdp-film.sh" Enter
Sleep 2s
Type "sdp_film_prepare ch79; sdp_film_ch79_setup; PS1=$'\e[35m❯\e[0m '; clear" Enter
Wait@240s
Sleep 1s
Show

# beat: envs | A static key in the spec fails each flow; then one project, two specs
Type "tail -2 spark-pipeline.static.yml   # a static key in the spec" Enter
Sleep 2s
Type "spark-pipelines run $R --spec spark-pipeline.static.yml 2>&1 | count_errors" Enter
Wait@180s
Sleep 4s
Type "tail -3 envs/prod.yml   # prod: its own database, storage and partitions" Enter
Sleep 3s
Type "spark-pipelines run $R --spec envs/dev.yml 2>&1 | show_probe" Enter
Wait@180s
Sleep 2s
Type "spark-pipelines run $R --spec envs/prod.yml 2>&1 | show_probe" Enter
Wait@180s
Sleep 8s
Hide
Type "sdp_film_end" Enter
Wait@60s

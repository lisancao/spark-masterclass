# Spark Masterclass, Chapter 78: the spark-pipelines CLI and the project.
#   film render code/film/ch78.tape --format chapter
Set TypingSpeed 35ms
Set WaitPattern /❯ ?$/

Hide
Type "bash --norc" Enter
Sleep 500ms
Type "export BOOK_DATA=${BOOK_DATA:-$HOME/spark-masterclass/data} CONNECT_PORT=15104 SPARK_UI_PORT=4144; . $FILM_DEMO/film/sdp-film.sh" Enter
Sleep 2s
Type "sdp_film_prepare ch78; mkdir -p transformations/sub; PS1=$'\e[35m❯\e[0m '; clear" Enter
Wait@240s
Sleep 1s
Show

# beat: cli | The spec from a subfolder, a refused glob, a README in transformations
Type "cd transformations/sub   # the CLI looks for the spec in each parent folder" Enter
Sleep 1s
Type "spark-pipelines dry-run $R 2>&1 | grep -E 'Loading|Run is' | short" Enter
Wait@120s
Sleep 3s
Type "cd ../.. && sed -i 's#transformations/\*\*#transformations/**/*.sql#' spark-pipeline.yml" Enter
Sleep 1s
Type "spark-pipelines dry-run $R 2>&1 | grep -o 'PIPELINE_SPEC_INVALID_GLOB.*' | short" Enter
Wait@120s
Sleep 4s
Type "sed -i 's#/\*\*/\*.sql#/**#' spark-pipeline.yml" Enter
Type "echo '# notes' > transformations/README.md   # not .py or .sql" Enter
Sleep 1s
Type "spark-pipelines dry-run $R 2>&1 | grep -o 'PIPELINE_UNSUPPORTED.*' | short" Enter
Wait@120s
Sleep 8s
Hide
Type "sdp_film_end" Enter
Wait@60s

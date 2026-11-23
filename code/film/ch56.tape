# Spark Masterclass, Chapter 56: your first ETL pipeline. Storyboard: notes/v2-sdp.md (films).
#   film render code/film/ch56.tape --format chapter
Set TypingSpeed 35ms
Set WaitPattern /❯ ?$/

Hide
Type "bash --norc" Enter
Sleep 500ms
Type "export BOOK_DATA=${BOOK_DATA:-$HOME/spark-masterclass/data} CONNECT_PORT=15104 SPARK_UI_PORT=4144; . $FILM_DEMO/film/sdp-film.sh" Enter
Sleep 2s
Type "sdp_film_prepare ch56; spark-pipelines run $R >/dev/null 2>&1; PS1=$'\e[35m❯\e[0m '; clear" Enter
Wait@240s
Sleep 1s
Show

# beat: day2 | A second day arrives; the next run reads only the new file
Type "counts      # after the first run: one day of trips" Enter
Wait@60s
Sleep 3s
Type "python make_days.py data/trips 2026-06-02" Enter
Wait@60s
Sleep 2s
Type "spark-pipelines run $R 2>&1 | ev" Enter
Wait@120s
Sleep 3s
Type "counts      # raw_trips = 111,546 + 126,260: the old file was not read again" Enter
Wait@60s
Sleep 8s
Hide
Type "sdp_film_end" Enter
Wait@60s

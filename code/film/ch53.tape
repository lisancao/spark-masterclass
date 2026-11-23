# Spark Masterclass, Chapter 53: Kafka with Spark in depth. Broker: ch53/kafka.sh start.
#   film render code/film/ch53.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 FILM_RUN_QUIET=1 FILM_RUN_PYTHON=.venv/bin/python SPARK_LOCAL_IP=127.0.0.1 SPARK_UI_PORT=4156 BOOK_DATA=$HOME/spark-masterclass/data && setopt interactivecomments && alias nvim='nvim -u $FILM_KIT/nvim-film.lua' && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show


# beat: dataloss | Records leave the topic before the query reads them
Type "# the broker deletes 400 unread records; the query restarts on its checkpoint" Enter
Sleep 500ms
Type "python ch53/k6_dataloss.py 2>/dev/null | head -18" Enter
Wait@240s
Sleep 12s

# beat: sink | A replayed batch writes its records again
Type "# copy a topic with the Kafka sink; replay batch 2; count what the topic holds" Enter
Sleep 500ms
Type "python ch53/k7_sink.py 2>/dev/null | head -16" Enter
Wait@300s
Sleep 12s

# beat: security | SASL_PLAINTEXT, a wrong password, a disallowed option
Type "# read and write as alice over SASL; then a wrong password; then a banned option" Enter
Sleep 500ms
Type "python ch53/k9_security.py 2>/dev/null | head -22" Enter
Wait@300s
Sleep 12s

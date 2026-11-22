# Spark Masterclass, Chapter 36: text analytics without an LLM. Storyboard: notes/part07.md.
#   film render code/film/ch36.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 BOOK_DATA=$HOME/spark-masterclass/data SPARK_UI_PORT=4227 SPARK_LOCAL_IP=127.0.0.1 && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: search | TF-IDF search, then every inbox ticket against the labelled tickets
Type "# TF-IDF search over four policies, then 240 tickets scored in the JVM" Enter
Sleep 500ms
Type "python ch36/text_search.py 2>/dev/null | sed -n '/search the policies/,$p' | cut -c1-92" Enter
Wait@900s
Sleep 10s

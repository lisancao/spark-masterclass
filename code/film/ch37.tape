# Spark Masterclass, Chapter 37: similarity search. Storyboard: notes/part07.md.
#   film render code/film/ch37.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 BOOK_DATA=$HOME/spark-masterclass/data SPARK_UI_PORT=4227 SPARK_LOCAL_IP=127.0.0.1 && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: banding | MinHash bands built from DataFrame functions: recall against work
Type "# MinHash with b bands of r rows: candidates and recall for pairs with J >= 0.7" Enter
Sleep 500ms
Type "python ch37/indexes.py 2>/dev/null | sed -n '/== banding/,/numHashTables=25/p' | cut -c1-92" Enter
Wait@1200s
Sleep 12s

# beat: ivf | The IVF recall curve
Type "clear" Enter
Type "# IVF over 20,000 vectors: recall at 10 against the share of vectors scored" Enter
Sleep 500ms
Type "python ch37/indexes.py 2>/dev/null | sed -n '/list sizes/,$p' | cut -c1-92" Enter
Wait@1200s
Sleep 10s

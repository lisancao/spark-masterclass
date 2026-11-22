# Spark Masterclass, Chapter 38: entity resolution and connected components. Storyboard: notes/part07.md.
#   film render code/film/ch38.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 BOOK_DATA=$HOME/spark-masterclass/data SPARK_UI_PORT=4227 SPARK_LOCAL_IP=127.0.0.1 && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: plan-growth | Label propagation without a cut, then with localCheckpoint()
Type "# label propagation: the analyzed plan after each round, without and with a cut" Enter
Sleep 500ms
Type "python ch38/resolve.py 2>/dev/null | sed -n '/without a cut/,/answer key has/p' | cut -c1-92" Enter
Wait@900s
Sleep 10s

# beat: cache-plan | Four ways to keep each round
Type "clear" Enter
Type "# the same loop kept four ways: nothing, cache(), localCheckpoint(), checkpoint()" Enter
Sleep 500ms
Type "rm -rf $BOOK_DATA/ch38; python ch38/components.py loop 2>/dev/null | sed -n '/^   nothing/,/^   localCheckpoint/p'" Enter
Wait@900s
Sleep 10s

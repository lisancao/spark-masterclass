# Spark Masterclass, Chapter 87: Spark on object storage.
#   code/ch87/minio.sh up          (MinIO in Docker; committers.py has written s3a://book/commit/)
#   film render code/film/ch87.tape --format chapter
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 BOOK_DATA=$HOME/spark-masterclass/data SPARK_UI_PORT=4155 SPARK_LOCAL_IP=127.0.0.1 && setopt interactivecomments && alias python=.venv/bin/python" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: failure | A job fails in its last task: what each committer leaves behind
Type "# 8 tasks write to MinIO. Task 7 fails after tasks 0 to 6 commit." Enter
Sleep 500ms
Type "python ch87/failure.py 2>/dev/null | head -7" Enter
Wait@300s
Sleep 10s

# beat: success | The magic committer's _SUCCESS file is a JSON report
Hide
Type "clear" Enter
Show
Type "# FileOutputCommitter writes an empty _SUCCESS. The S3A committers write JSON:" Enter
Sleep 500ms
Type "python ch87/success.py" Enter
Wait@60s
Sleep 8s

# beat: dpo | Dynamic partition overwrite needs a rename-based committer
Hide
Type "clear" Enter
Show
Type "# INSERT OVERWRITE of one partition, with the magic committer and with v1:" Enter
Sleep 500ms
Type "python ch87/dpo.py 2>/dev/null | cut -c1-94" Enter
Wait@300s
Sleep 10s

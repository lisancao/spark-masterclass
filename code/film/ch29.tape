# Spark Masterclass, Chapter 29: tables. Storyboard: notes/part06.md (film section).
#   film render code/film/ch29.tape --format chapter   (after ch29/delta_log.py and ch29/iceberg_log.py ran)
Set TypingSpeed 40ms
Set WaitPattern /❯$/

# Setup, off camera at the start of every clip.
Hide
Type "cd $FILM_DEMO && export VIRTUAL_ENV_DISABLE_PROMPT=1 W=${BOOK_DATA:-$HOME/spark-masterclass/data}/work/part06 && setopt interactivecomments" Enter
Type "[[ -n $FILM_REAL ]] || PROMPT='%F{magenta}❯%f '; clear" Enter
Sleep 1s
Show

# beat: delta | The Delta log: one JSON file per commit
Type "cd $W/ch29-delta/employees && ls _delta_log | head -4" Enter
Sleep 2s
Type "# version 1 was the MERGE: which actions did it write?" Enter
Sleep 500ms
Type "jq -c 'keys[0]' _delta_log/00000000000000000001.json" Enter
Sleep 2s
Type `jq -c '(.add // .remove) | select(.) | {path: .path[0:22], dataChange, rows: (.stats // "{}" | fromjson | .numRecords)}' _delta_log/00000000000000000001.json` Enter
Sleep 9s

# beat: iceberg | The Iceberg tree: metadata file, snapshots, manifests
Hide
Type "clear" Enter
Show
Type `cd $W/ch29-iceberg/hr/employees/metadata && echo "version-hint: $(cat version-hint.text)  avro files: $(ls | grep -c avro)"` Enter
Sleep 2s
Type "# the root of the tree: every snapshot, and its parent" Enter
Sleep 500ms
Type `jq -c '.snapshots[0:3][] | {id: ."snapshot-id", parent: ."parent-snapshot-id", op: .summary.operation, files: .summary."total-data-files"}' v$(cat version-hint.text).metadata.json` Enter
Sleep 10s

# Storyboard: Part I chapter clips

Chapter format (1280x720, about 95x24 cells), embedded in the book's pages. VO lines are drafts, checked against the
scripts' output; the clips are muted in the book and the VO guide is for a narrated cut later.

## ch01.tape

| Beat | On screen | VO (draft) | Has to be readable |
|---|---|---|---|
| query | `anatomy.py` at the query | This query asks for trips and average cost by hour. Building it runs nothing; Spark only records a plan. | the `where`, `groupBy`, `agg`, `orderBy` chain |
| jobs | the jobs and stages one `collect()` became | One collect becomes four jobs, because adaptive execution runs the query a stage at a time and re-plans after each shuffle. | `job 1` to `job 4`, `tasks 16` then `tasks 1` |
| tasks | the scan stage, task by task | Sixteen tasks scan the file, and only four of them read rows, because the file has four row groups. | the four non-zero rows, `4 of 16`, `4 row groups` |
| feather | single-task execution off and on | On a small query, Spark 4.3's single-task execution drops the shuffles: two jobs instead of five, and about forty percent faster here. | both ms figures, `5.0 jobs` and `2.0 jobs` |

## ch04.tape

| Beat | On screen | VO (draft) | Has to be readable |
|---|---|---|---|
| connect | a query and a bad column over Connect | Over Spark Connect the client only sends a plan. The bad column isn't caught until the server analyzes it, and in 4.3 the error carries an operation ID. | `no error yet`, both operation IDs, the error class |
| rest | the failed operation in the REST API | The server keeps a record of the same operation, found by that ID: its state, timing and the full error. | `operationId`, `state`, `detail` |

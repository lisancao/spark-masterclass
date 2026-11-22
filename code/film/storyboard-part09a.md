# Storyboard: Part IX chapter clips, chapters 47 to 51

Chapter format (1280x720, about 95x24 cells), embedded in the book's pages. The VO lines are drafts, checked against the scripts' output. The clips are muted in the book.

## ch50.tape

| Beat | On screen | VO (draft) | Has to be readable |
|---|---|---|---|
| where | the trace of a Python data source | Spark runs each method of a data source in a Python worker. load() runs schema(). The first action plans the partitions. The tasks run read(). | the driver pid, the other pids, the three sections |
| balance | round robin against largest-first planning | Round robin gives each task the same number of files. Largest-first gives each task the same number of bytes. | both lists of (files, bytes), the bytes per task table |
| errors | when each error appears | Planning errors appear at the first action as PYTHON_DATA_SOURCE_ERROR. Errors in read() appear in a task as PYTHON_EXCEPTION. | each label, `fails at build` or `fails at action`, the condition |

## ch51.tape

| Beat | On screen | VO (draft) | Has to be readable |
|---|---|---|---|
| requests | the requests of each step against the tickets API | load() and explain() send nothing. The first count plans and reads. Each later action reads again, until you cache. | each request count and the page list |
| ranges | JDBC stride ranges and quantile ranges | JDBC splits the key space evenly. Two late keys put almost all rows in one task. Quantiles split the rows evenly. | the four WHERE clauses, both rows-per-partition lists |
| timezone | one naive datetime under three worker time zones | A naive datetime takes the time zone of the worker. An aware one stays correct. | the three naive values, the aware value |

## ch52.tape

| Beat | On screen | VO (draft) | Has to be readable |
|---|---|---|---|
| offered | the filter objects for each WHERE condition | Spark adds IsNotNull, rewrites NOT, BETWEEN and LIKE, and never offers OR. | each condition and its filters |
| opened | rows, files opened and Spark's Filter for each query | The audit proves the pushdown: five files opened, not twenty-six. | the opened column, the Filter column |
| shared | the shared planned read | After a filtered query, a plain count on the same DataFrame returns five, not twenty-six. | 5 vs 26, 10 vs 31 |

## ch53.tape

| Beat | On screen | VO (draft) | Has to be readable |
|---|---|---|---|
| commit | the writer trace, append, overwrite and a failed append | The tasks stage files, the commit publishes them. A failed job calls abort with None for each task, and the manifest does not change. | pids, the manifest lines, abort([None, None, None]) |
| stream | the stream calls, the checkpoint, restart and replay | Offsets go to the checkpoint before a batch, commits after it. A replayed batch is skipped by the sink. | the batch lists, the offset JSON, "replay skipped" |

## ch54.tape

| Beat | On screen | VO (draft) | Has to be readable |
|---|---|---|---|
| workers | the lookup with PYSPARK_PYTHON unset, then with bare workers | The package is installed, but the workers use another Python. | DATA_SOURCE_NOT_FOUND, ModuleNotFoundError |
| logs | python_worker_logs() after a read | Warnings and info lines from read() come back as rows. A print with three arguments gives three rows. | the level, logger and msg columns |

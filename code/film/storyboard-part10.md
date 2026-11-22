# Storyboard: Part X chapter clips

Chapter format (1280x720, about 95x24 cells), embedded in the book's pages. VO lines are drafts, checked against the
scripts' output; the clips are muted in the book and the VO guide is for a narrated cut later.

## ch56.tape

| Beat | On screen | VO (draft) | Has to be readable |
|---|---|---|---|
| twice | `twice.py`: rows and UDF calls for each query | A filter on a UDF column copies the UDF call, so the UDF runs more times than there are rows: 270 calls for 200 rows. | `270`, `129`, `0` for count() |
| transpile | `transpile.py`: which UDFs the 4.3 transpiler makes native | With transpilation on, a one-expression UDF becomes a native expression. A two-statement function or a module call stays Python, and Spark says why. | `native`, `stays Python`, the reasons |

## ch57.tape

| Beat | On screen | VO (draft) | Has to be readable |
|---|---|---|---|
| tap | `tap.py`: the annotated response of a pickled UDF | The worker answers with a pickle of the results, four timing longs, two spill longs, the accumulators, and minus four: use me again. | `result frame`, `TIMING_DATA`, `END_OF_STREAM` |
| failures | `failures.py`: six failing UDFs and what the driver gets | A Python exception comes back as data with its traceback. A dead worker can send nothing, so the driver sees only an end of file. | `PYTHON_EXCEPTION`, `worker_crashed`, `idle_timeout` |

## ch59.tape

| Beat | On screen | VO (draft) | Has to be readable |
|---|---|---|---|
| repro | `repro.py`: the metric view and the regular view from `default` and from `other` | From the namespace it was made in, the metric view is right. After USE other, it reads another table, and the regular view next to it stays right. | `(east, 999)` against `(east, 10), (west, 20)` |
| fix | `test_fix.sh`: patch, compile against the wheel's jars, rerun | Two patched Scala files, compiled with the compiler inside the pyspark wheel, put first on the class path: every case now gives the right rows. | `compiled ... 169 class files`, all rows `(east, 10), (west, 20)` |

## ch58.tape

| Beat | On screen | VO (draft) | Has to be readable |
|---|---|---|---|
| layout | `layout.py`: the 44 value bytes of one event, decoded | The header says object, one-byte ids and offsets. The ids are sorted by name; the values stay in the order of the JSON text. | `ids [2, 0, 3, 1]`, the four decoded fields |
| handmade | `handmade.py`: hand-made bytes, then unsorted ids at 5 to 40 fields | Spark checks bounds, not the sort rule. From 32 fields it switches to binary search, and a lookup in an unsorted object returns NULL. | `MALFORMED_VARIANT`, `k00=None` from 32 fields |

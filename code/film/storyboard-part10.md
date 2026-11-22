# Storyboard: Part X chapter clips

Chapter format (1280x720, about 95x24 cells), embedded in the book's pages. VO lines are drafts, checked against the
scripts' output; the clips are muted in the book and the VO guide is for a narrated cut later.

## ch56.tape

| Beat | On screen | VO (draft) | Has to be readable |
|---|---|---|---|
| twice | `twice.py`: rows and UDF calls for each query | A filter on a UDF column copies the UDF call, so the UDF runs more times than there are rows: 270 calls for 200 rows. | `270`, `129`, `0` for count() |
| transpile | `transpile.py`: which UDFs the 4.3 transpiler makes native | With transpilation on, a one-expression UDF becomes a native expression. A two-statement function or a module call stays Python, and Spark says why. | `native`, `stays Python`, the reasons |

# partitions() deals files to tasks

Claim: largest-first planning gives each task about the same bytes; round robin does not.
Data: `data.json`, per-file sizes of the 26 sample files that `code/ch50/docs_source.py` copies; the totals agree with `code/ch50/output/docs_source.txt`.

| beat | start | what moves | caption | why |
|---|---|---|---|---|
| rr | 0.4 | files dealt in turn | Round robin gives each task the same number of files. | the naive plan |
| skew | 4.5 | totals 22,793 / 10,884 / 30,098 | The bytes differ a lot. The stage waits for the slowest task. | the cost |
| lf | 7.4 | files dealt largest first | Largest first puts each file on the smallest stack. | the greedy rule |
| even | 11.2 | totals 21,266 / 21,261 / 21,248 | Now each task reads about 21,250 bytes. | the result |

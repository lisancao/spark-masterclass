# A task talks to a Python worker

Claim: one task is one request (init message, data frames, end markers) and one response (results, timings, spill, accumulators, END_OF_STREAM), and -4 lets the JVM reuse the worker.
Data: `data.json`, frame sizes of the pickled UDF in `code/ch60/output/tap.txt`.

| beat | start | what moves | caption | why |
|---|---|---|---|---|
| init | 0.4 | -8 and the init message | The JVM starts each request with -8 and the init message. | the framing |
| data | 3.2 | data frame, -1, -4 | The rows follow. Negative lengths mark the ends of sections. | markers |
| response | 7.2 | results to the JVM | The worker answers with results, timings, spill and accumulators. | the reply |
| reuse | 10.6 | -4 | It ends with -4, so the JVM can use the worker again. | reuse |

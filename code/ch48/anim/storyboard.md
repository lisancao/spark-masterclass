# Animation: a spec value goes to the session and to each flow; the decorator value did not reach the write

**Claim.** Chapter 48, Step 2 and Step 10.

**Numbers.** `code/ch48/output/configuration.txt` (spec 3, 3 files), `flowconf.txt` (decorator 2, still 3 files).

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `two-paths` | 1.0 | dots from spec to session and graph | The CLI sends the value to the session and to the graph. | same | the two paths |
| `flow` | 4.6 | dots into the flow, decorator dot | Each flow merges the graph value, then its own spark_conf. | same | the source rule |
| `write` | 8.6 | session dot to the write, "3 files" | In the test, the write used 3 partitions, not 2: 3 files. | same | the measured result |

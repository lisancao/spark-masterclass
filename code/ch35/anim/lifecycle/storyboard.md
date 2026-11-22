# Animation: one UDTF instance per partition: __init__, eval per row, terminate once, cleanup last

**Claim.** Spark makes one instance of the UDTF for each partition, calls eval for each row, terminate once at the end (its row joins to the last input row) and cleanup last. From Chapter 35, Step 2.

**Numbers.** `data.json`, from `code/ch35/output/udtf_core.txt`: 6 rows in 3 partitions, 2 eval calls per instance, 27 rows, the terminate row `('doc 1 has words', '<end>', 2)`. Timing schematic.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | partition rows, instance appears with __init__ | One partition with two rows gets one Words instance | One partition with two rows gets one Words instance. | one instance per partition |
| `eval1` | 4.8 | first row goes in, four rows come out | Each eval call yields one row for each word | Each eval call yields one row for each word. | eval per row, many rows out |
| `eval2` | 9.6 | second row, then terminate adds the end row | terminate runs once at the end and joins to the last row | terminate runs once at the end and joins to the last row. | where terminate rows attach |
| `cleanup` | 14.4 | cleanup, instance dims, total | cleanup runs last, also after a failure (stays) | cleanup runs last, also after a failure. | ends on the measured total |

# Animation: Arrow batches from the Parquet reader cross to the Python worker, one UDF call per batch

**Claim.** With columnar input (the 4.2+ default), each Parquet batch of 4,096 rows goes to the Python worker as one Arrow batch, and a pandas UDF runs once per batch. From Chapter 20, Step 6.

**Numbers.** `data.json`, from `code/ch20/output/arrow_batches.txt`: 4,096 rows x 937 batches by default, 10,000 rows x 385 batches with columnar input off. Timing schematic.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | JVM task, reader, socket, worker | The Parquet reader gives the task batches of 4,096 rows | The Parquet reader gives the task batches of 4,096 rows. | where the batches come from |
| `cross` | 4.8 | one batch goes over, results come back | Each batch crosses to the worker as one Arrow batch | Each batch crosses to the worker as one Arrow batch. | the boundary |
| `once` | 9.6 | six batches, the function pulses on each | The pandas UDF runs once per batch, not once per row | The pandas UDF runs once per batch, not once per row. | why pandas UDFs are cheap per row |
| `result` | 14.4 | table of measured batch counts | The Parquet batch size sets the Python batch size (stays) | The Parquet batch size sets the Python batch size. | ends on the measured numbers |

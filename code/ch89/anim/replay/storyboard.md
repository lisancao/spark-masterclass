# Animation: a replayed batch writes its records to Kafka again

**Claim.** The Kafka sink writes at least once: a batch that ran without its commit runs again and sends the same records a second time; a reader that keeps one record per key gets the input back. From Chapter 89, Step 7.

**Numbers.** `data.json`, from `code/ch89/output/k7_sink.txt`: batches (0, 498), (1, 498), (2, 449); 1,894 records in the topic; 1,445 distinct keys. The time between steps is schematic.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `write` | 0.0 | batches 0 and 1 fill the topic, each followed by its commit | Each batch writes its records, then Spark writes a commit | Each batch writes its records, then Spark writes a commit. | the normal protocol |
| `crash` | 5.0 | batch 2 writes 449 records; the commit slot turns red | A crash after the write leaves batch 2 without a commit | A crash after the write leaves batch 2 without a commit. | the failure point |
| `replay` | 10.0 | batch 2 runs again; 449 more records; counter 1,894 | The restart runs batch 2 again and sends the same records | The restart runs batch 2 again and sends the same records. | the duplicate |
| `dedup` | 15.0 | reader keeps one record per key; counter 1,445 | A reader that keeps one record per key gets the input back | A reader that keeps one record per key gets the input back. | the fix, measured |

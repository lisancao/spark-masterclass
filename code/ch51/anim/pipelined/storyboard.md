# Animation: micro-batch runs its stages in sequence per batch; Real-time Mode runs all tasks at once with a pipelined shuffle

**Claim.** In Real-time Mode the scan tasks and the aggregate tasks run at the same time for the whole batch, and each record goes through the pipelined shuffle as it arrives. Micro-batch collects records and runs two stages one after the other. From Chapter 51, Step 3 and Step 4.

**Numbers.** `data.json`: 4 scan tasks and 3 aggregate tasks (`r4_pipeline.txt`), p50 of the count 276 ms and 28 ms (`r2_latency.txt` medians). Timing schematic.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | two lanes, idle tasks | The same query in micro-batch mode and in Real-time Mode | The same query in micro-batch mode and in Real-time Mode. | sets up the comparison |
| `microbatch` | 4.8 | records wait, stages light in sequence | Micro-batch: records wait, then two stages run in sequence | Micro-batch: records wait, then two stages run in sequence. | the latency floor |
| `realtime` | 9.6 | all seven tasks lit, records flow straight | Real-time Mode: all tasks run, and each record goes straight through | Real-time Mode: all tasks run, and each record goes straight through. | the pipelined shuffle |
| `result` | 14.4 | measured medians | The pipelined shuffle sends rows while both stages run (stays) | The pipelined shuffle sends rows while both stages run. | ends on measured numbers |

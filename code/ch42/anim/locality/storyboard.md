# Animation: delay scheduling with spark.locality.wait=3s

**Claim.** An idle executor waits for the locality timer before it runs a task whose cached block is on another executor. From Chapter 42, Step 12.

**Numbers.** From `code/ch42/output/locality.txt`, the default run: executor A holds 7 cached partitions, executor B holds 1; B runs its own partition at +0.0 s, then waits, then runs partition 5 at NODE_LOCAL at +3.5 s; job 2 took 6.7 s. Task lengths are schematic (1 s each). The timer bar is schematic.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | why this beat |
|---|---|---|---|---|
| `open` | 0.0 | two executors, 8 cached blocks (7 on A, 1 on B) | Job 2: eight 1 s tasks. A caches 7 blocks, B caches 1 | the input |
| `local` | 3.0 | p0 runs on A, p1 runs on B, both PROCESS_LOCAL | Each executor first runs the tasks of its own blocks | locality first |
| `wait` | 6.5 | B is idle; a timer fills to 3 s | B is free, but every waiting task prefers A. B waits for spark.locality.wait | delay scheduling |
| `steal` | 11.0 | p5 moves to B, labelled NODE_LOCAL | At 3.5 s the timer expires, and B runs p5 at NODE_LOCAL | the fallback |
| `result` | 15.0 | totals | job 2: 6.7 s; with spark.locality.wait=0: 5.4 s (stays) | measured effect |

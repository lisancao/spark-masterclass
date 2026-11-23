# Animation: fit() of an iterative estimator is a loop of treeAggregate jobs

**Claim.** Each job sends the coefficients to the tasks, merges partial sums in a tree, and the driver takes one optimizer step. l-bfgs ran 11 jobs for 6 iterations; the normal solver ran 2 (Chapter 63, Step 4).

**Numbers.** `data.json`, from `code/ch63/output/estimators.txt`. Four partitions and the timing are schematic and say so on screen.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | driver and four partitions | fit() is a loop of treeAggregate jobs (stays) | Fit is not one operation. | subject at frame 0 |
| `out` | 0.8 | coefficients fly to the tasks | The driver sends the coefficients to each task | The driver sends the coefficients to each task. | the broadcast half |
| `sums` | 2.4 | partial sums merge and go up | Each task returns a loss and a gradient, merged in a tree | Each task returns a loss and a gradient, merged in a tree. | the tree merge |
| `loop` | 4.0 | job counter rises to 11, fast passes | The driver takes one optimizer step, and the next job starts | Then the driver takes a step, and the next job starts. | the loop |
| `result` | 10.2 | counter stays at 11 | l-bfgs: 11 jobs for 6 iterations. normal: 2 jobs. (stays) | Eleven jobs for six iterations. | measured result |

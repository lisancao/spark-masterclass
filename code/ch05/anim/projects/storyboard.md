# Animation: a withColumn loop builds a deep plan that the driver analyzes again and again

**Claim.** Each `withColumn` call adds a `Project` and the driver analyzes the new plan; `withColumns` adds one `Project`; the optimizer collapses both to one, so only driver time differs. From Chapter 5, Steps 4 and 5.

**Numbers.** 7,582.4 ms and 703.5 ms at 400 columns, `code/ch05/output/plans.txt` (median of 7 after a warmup). The six drawn calls are schematic, labelled on screen.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | title, the two base plans | Why a withColumn loop gets slow (stays) | Two ways to add columns. | topic at frame 0 |
| `loop` | 0.9 | six Projects stack up, each followed by a sweep over the whole plan | Each call adds a Project, and the driver analyzes the new plan | Each call adds a Project, and the driver analyzes the new plan. | the mechanism |
| `once` | 7.0 | one Project, one sweep | withColumns adds one Project for all the columns | withColumns adds one Project for all the columns. | the contrast |
| `collapse` | 9.5 | the stack collapses into one Project on both sides | The optimizer then merges the stack into one Project | The optimizer then merges the stack into one Project. | same tasks |
| `cost` | 11.7 | measured times | Same tasks, but driver time at 400 columns differs (stays) | Same tasks, but driver time at 400 columns differs. | ends on the measurement |

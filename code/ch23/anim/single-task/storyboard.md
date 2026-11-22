# Animation: single-task execution removes both Exchanges and the partial aggregate from a small query

**Claim.** With the setting on, the scan of one small file reports SinglePartition, so EnsureRequirements adds no Exchange, the aggregate has one phase, and the query is one job of one task. From Chapter 23, Step 3 and Step 4.

**Numbers.** `data.json`, from `code/ch23/output/plans_aqe.txt`: off 4 jobs, 4 stages, 4 tasks, 1,172 B shuffle write, 1,933 ms; on 1/1/1, 0 B, 340 ms. Timing schematic.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | plan chain with two Exchanges | Off: two shuffles, and the aggregate runs in two phases | Off: two shuffles, and the aggregate runs in two phases. | the plan to change |
| `single` | 4.8 | scan pulses, SinglePartition label | On: the one-file scan reports a single partition | On: the one-file scan reports a single partition. | the cause |
| `collapse` | 9.6 | Exchanges and partial aggregate leave, chain closes | No Exchange is needed, and one complete count is enough | No Exchange is needed, and one complete count is enough. | the effect on the plan |
| `result` | 14.4 | table of UI records | One job, one stage, one task (stays) | One job, one stage, one task. | ends on the measured records |

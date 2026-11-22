# Animation: a pipeline run resolves the graph, then runs each flow when its inputs are complete

**Claim.** The server resolves the full graph first; then each flow starts when its inputs are complete, and each table gets its rows. From Chapter 46, Step 7.

**Numbers.** Flow order from `code/ch46/output/07-run1.txt`; row counts from `08-look1.txt`. Timing between events is schematic and labelled.

**Backend:** hf. **Themes:** ide. **Formats:** landscape.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | title and the six boxes on screen | A pipeline run | A pipeline run. | the topic reads at frame 0 |
| `resolve` | 1.2 | edges of the graph draw in | First, the server resolves the whole graph. | First, the server resolves the whole graph. | resolution comes before any flow |
| `flows` | 4.0 | raw_trips and zones run together, then clean_trips, then borough_daily | Each flow starts when its inputs are complete. | Each flow starts when its inputs are complete. | the order is the point |
| `tables` | 10.0 | all four tables green with row counts | Each table now holds its rows. The next run reads only new files. | Each table now holds its rows, and the next run reads only new files. | ends on real numbers |

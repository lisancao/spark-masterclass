# Animation: TriggeredGraphExecution runs the flows of run 2 in topological order

**Claim.** SDP prepares all tables first, then a flow starts only when its inputs are done (Chapter 62, Step 6).

**Numbers.** `data.json`: the COMPLETED times of run 2 in `code/ch62/output/end_to_end.txt` (1 s resolution), played at 0.8x.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | graph of flows, all queued | Run 2: each flow waits for its inputs (stays) | This is run two of the shop pipeline. | subject at frame 0 |
| `materialize` | 0.6 | nothing runs yet | First DatasetManager prepares every table | First DatasetManager prepares every table. | step 4 comes before step 5 |
| `execute` | 2.6 | nodes turn green at their COMPLETED times | Then a flow starts when its inputs are done | Then a flow starts when its inputs are done. | the scheduler |
| `done` | 10.6 | all green | Run is COMPLETED (stays) | The run is complete. | the end of a triggered run |

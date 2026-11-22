# Animation: a UDF records its include list one time, and a reused worker keeps a path

**Claim.** The old UDF keeps an empty include list after addPyFile; a new UDF puts the zip on the sys.path of the reused workers, so the old UDF works only on those workers (Chapter 44, Step 5).

**Numbers.** `data.json`, from `code/ch44/output/pyfiles.txt` (two worker PIDs, the import results in order). Timing is schematic.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | old UDF, two workers | A UDF records its include list one time (stays) | One UDF, two workers. | subject at frame 0 |
| `first` | 0.8 | tasks fail | The old UDF records an empty include list | The old UDF records an empty include list. | the capture |
| `add` | 3.6 | addPyFile, tasks still fail | After addPyFile, the old UDF still sends no include | After addPyFile, nothing changes for it. | the trap |
| `new` | 7.2 | new UDF, workers get the zip | A new UDF adds the zip to the reused workers | A new UDF adds the zip to the workers. | the fix, and the side effect |
| `leftover` | 10.4 | old UDF works | Now the old UDF works, but only on these two workers (stays) | Now the old one works, but only here. | why it fails on a cluster |

# Animation: a broadcast join copies the small side; a sort-merge join shuffles both sides

**Claim.** The broadcast hash join leaves the trips where they are and sends a hash table of the zones to each task; the sort-merge join moves both sides by key. From Chapter 9, Steps 2 and 3.

**Numbers.** 0.17 s and 0.75 s from `code/ch09/output/strategies.txt` (section 3, median of 3). Dots and partitions are schematic, labelled on screen.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | title, both lanes | Two ways to join trips to zones (stays) | Two ways to join trips to zones. | topic at frame 0 |
| `broadcast` | 0.9 | hash tables fly to each task | Broadcast: a copy of the small side goes to each task. The trips stay. | Broadcast: a copy of the small side goes to each task. The trips stay. | the cheap case |
| `shuffle` | 5.0 | trip and zone dots fly to reduce tasks | Sort-merge: a shuffle moves both sides by the key, then each task sorts | Sort-merge: a shuffle moves both sides by the key, then each task sorts. | the costly case |
| `cost` | 9.6 | measured times | The same join and the same answer, measured on the June trips (stays) | The same join and the same answer, measured on the June trips. | ends on numbers |

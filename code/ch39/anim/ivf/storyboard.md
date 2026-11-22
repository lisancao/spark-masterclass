# Animation: IVF probes the nearest lists; recall and work rise with nprobe

**Claim.** IVF compares the query with 64 centroids and scores only the vectors in the nprobe nearest lists; recall and work rise together (Chapter 39, Step 8).

**Numbers.** `data.json`, from `code/ch39/output/indexes.txt`. The 2-D positions are schematic and say so on screen.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | 64 lists and a query | IVF: search only the nearest lists (stays) | Twenty thousand vectors in sixty-four lists. | subject at frame 0 |
| `centroids` | 0.6 | nothing moves, the query is marked | k-means makes 64 lists. A query compares itself with the centroids | The query compares itself with the centroids. | the index step |
| `probe` | 3.2 | 1, 2, 4, 8, 16 nearest lists light up, table rows appear | More lists probed: higher recall, more vectors scored (stays) | More lists probed, higher recall, more work. | the measured trade |

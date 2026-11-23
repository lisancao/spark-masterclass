# Animation: each MapReduce-style pass reads the input again; with cache() only the first pass does

**Claim.** The same iterative job, ten passes, is about four times faster per pass with a cached RDD. From Chapter 1, Step 5.

**Numbers.** `code/ch01/output/iterative.txt`, copied into `data.js` by `../build_data.py`. The lanes run in real time.

**Backend:** hf (lanes and cells). **Themes:** ide. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | title and both lanes on screen | Ten passes over the same data (stays) | Ten passes over the same data, in two styles. | the topic reads at frame 0 |
| `run` | 0.6 | both lanes fill pass cells at measured speed, disk pulses on every MapReduce pass | Each MapReduce pass reads and parses all the input again | Each MapReduce pass reads and parses all the input again. | shows the repeated read |
| `cached` | 4.8 | the cached lane is done, the other still runs | cache() parses once, then every pass reads memory | cache() parses once, then every pass reads memory. | the contrast |
| `result` | 13.8 | totals and the median ratio | Same answer. Median pass 1.02 s against 0.26 s: 3.9x (stays) | Same answer, and each pass is about four times faster. | ends on measured numbers |

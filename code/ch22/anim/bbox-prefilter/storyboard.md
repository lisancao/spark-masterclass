# Animation: a grid key and a bounding box remove most point-zone pairs before the exact edge-crossing test

**Claim.** Spark 4.3.0 has no ST_Contains. A grid key and a bounding box drop most pairs, then a ray-crossing count gives the exact answer. Chapter 22, Steps 7 and 8.

**Numbers.** `data.json`, written by `code/ch22/anim_data.py` from `code/ch22/output/pip.txt` (pair counts) and `code/ch22/output/pip_bench.txt` (medians). The map is the real zones and Citi Bike stations of a window of lower Manhattan.

**Backend:** hf. **Themes:** paper. **Formats:** landscape (book). **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | map, stations and the counter on screen at frame 0 | Spark 4.3.0 has no ST_Contains, so you write the test yourself. | Spark 4.3.0 has no ST_Contains, so you write the test yourself. | the problem must read in the first frame |
| `pairs` | 4.8 | counter shows all pairs | Every station against every zone is a lot of pairs. | Every station against every zone is a lot of pairs. | the cost that the rest removes |
| `grid` | 9.4 | one-mile grid lines appear, counter drops | A one-mile grid key keeps only the zones in the same cell. | A one-mile grid key keeps only the zones in the same cell. | first filter, an equi-join key |
| `bbox` | 14.2 | one station in focus, the boxes of its cell's zones, most boxes leave | A bounding box drops most of the rest with four comparisons. | A bounding box drops most of the rest with four comparisons. | second filter, cheap comparisons |
| `exact` | 19.0 | a ray to the right, crossings marked, the containing zone fills | A ray from the point crosses the edges. An odd count means inside. | A ray from the point crosses the edges. An odd count means inside. | the exact test, shown on real edges |
| `result` | 24.4 | measured medians | Same answers: 2.94 s for all pairs, 0.24 s with grid and box. | Same answers: 2.94 seconds for all pairs, 0.24 with grid and box. | end on the measured numbers |

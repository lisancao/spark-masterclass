# Animation: reduceByKey combines in each map task, so the shuffle moves partial sums, not every value

**Claim.** `reduceByKey` adds the values of each key inside each map task, so the shuffle carries one partial sum per key per task. `groupByKey` carries every value. From Chapter 12, Step 3.

**Numbers.** 0.02 MiB and 11.76 MiB of shuffle write, `code/ch12/output/keyvalue.txt` (3,837,248 pairs, 258 zones). The two map tasks, two keys and six values per task on screen are schematic, labelled on screen.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | title, two lanes, map tasks with values | Two ways to total the fares of each zone | Two ways to total the fares of each zone. | topic at frame 0 |
| `combine` | 1.6 | top lane: values of one key merge into one dot in each task | reduceByKey adds each key's values inside the map task | reduceByKey adds each key's values inside the map task. | the mechanism |
| `shuffle` | 4.6 | top: two partial sums per task cross the shuffle; bottom: every value crosses | groupByKey sends every value across the shuffle | groupByKey sends every value across the shuffle. | the contrast |
| `sum` | 7.8 | reducers show totals; measured sizes appear | Same totals, measured on 3.8 million taxi fares (stays) | Same totals, measured on 3.8 million taxi fares. | ends on the measurement |

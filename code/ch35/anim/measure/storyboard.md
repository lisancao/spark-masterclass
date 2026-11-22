# Animation: MEASURE() puts the stored formula into the query, at the grain of the GROUP BY

**Claim.** ResolveMetricView replaces MEASURE(fare_per_mile) with SUM(fare_amount) / SUM(trip_distance); each grain computes it from the rows; an outer AVG over the results is wrong (Chapter 35, Steps 4 to 6).

**Numbers.** `data.json`, from `code/ch35/output/grains.txt` and `define.txt`.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | query and YAML box | The catalog holds the formula of each measure | The catalog holds the formula of each measure. | subject at frame 0 |
| `rewrite` | 3.4 | MEASURE() text changes into the formula | The analyzer replaces MEASURE() with that formula | The analyzer replaces MEASURE with that formula. | the mechanism |
| `grains` | 6.6 | month value and borough table appear | Each GROUP BY computes it again from the rows | Each group by computes it again from the rows. | correct at each grain |
| `trap` | 9.8 | AVG of the borough rows: 11.5498 | An AVG over these results is wrong again (stays) | An average over these results is wrong again. | the limit of the feature |

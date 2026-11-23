# Animation: ROLLUP is one Expand and one aggregate, not three queries

**Claim.** `GROUP BY ROLLUP(Borough, pay)` copies each row once per grouping set with an `Expand`, then one hash aggregate groups by the keys plus `spark_grouping_id`. Chapter 19, Step 6.

**Numbers.** `data.json`, copied from `code/ch19/output/sets.txt` sections 1 and 2 (rows into Expand = Filter output of the trips side after the join and filters = 2,738,727, Expand output 8,216,181, partial HashAggregate output 30, 10 result rows).

**Backend:** hf (boxes, text and counters). **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | title, one input row | ROLLUP asks for three levels of totals. | ROLLUP asks for three levels of totals. | sets the question |
| `expand` | 4.5 | Expand box, three copies with ids 0, 1, 3 | Expand copies each row once for each grouping set. | Expand copies each row once for each grouping set. | the mechanism |
| `count` | 9.5 | counters from SQL metrics | Three copies of 2.7 million rows: the aggregate sees 8.2 million. | Three copies of 2.7 million rows: the aggregate sees 8.2 million. | the cost, measured |
| `agg` | 14.5 | HashAggregate box, partial output 30 | Grouping by the keys and the id keeps each level apart. | Grouping by the keys and the id keeps each level apart. | why the id exists |
| `result` | 20.0 | result line | One scan and one shuffle give all three levels. (stays) | One scan and one shuffle give all three levels. | the payoff |

## What has to be readable

The three copies with their ids, both counters, the partial output count, the result line.

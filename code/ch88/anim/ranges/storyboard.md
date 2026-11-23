# Animation: JDBC strides are equal in key space, so one stray value puts all rows in one task

**Claim.** Spark cuts the range from lowerBound to upperBound into equal strides. With bounds from min and max, one trip from 2008 makes the strides years wide, and one task reads every June row. With bounds of the month, six tasks share the rows. From Chapter 88, Step 3.

**Numbers.** `data.json`, from `code/ch88/output/partitions.txt`. Timing schematic.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `axis` | 0.0 | a time axis 2008 to 2026, one dot in 2008, a dense block in June 2026 | One trip is from 2008. All others are from June 2026 | One trip is from 2008. All others are from June 2026. | the data |
| `strides` | 4.0 | six equal strides over the axis, row counts per task | Bounds from min and max: six strides of almost three years | Bounds from min and max give six strides of almost three years. | the skew |
| `june` | 9.0 | axis zooms to June, six strides, counts | Bounds of the month: each task reads about 640,000 rows | With the bounds of the month, each task reads about 640,000 rows. | the fix |

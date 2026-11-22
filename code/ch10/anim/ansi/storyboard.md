# Animation: ANSI mode stops the job at one zero fare; try_divide keeps going with a NULL

**Claim.** Under ANSI mode a division by zero in one row fails the job; `try_divide` returns NULL for that row and `avg` ignores it. From Chapter 10, Step 1.

**Numbers.** 3,167 zero fares, 0.1732 over 3,834,081 trips, `DIVIDE_BY_ZERO` / 22012: `code/ch10/output/ansi.txt`. Row values are schematic, labelled on screen.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | title, two lanes | One zero fare in 3.8 million trips (stays) | One zero fare in 3.8 million trips. | topic at frame 0 |
| `ansi` | 0.8 | rows reach the task; the zero stops it; the error shows | Under ANSI mode, one zero fare stops the whole job | Under ANSI mode, one zero fare stops the whole job. | the failure |
| `try` | 6.0 | rows pass; the zero becomes NULL and fades at avg | try_divide gives NULL for that row, and avg ignores NULL | try_divide gives NULL for that row, and avg ignores NULL. | the fix |
| `result` | 11.4 | the measured average | 3,167 zero fares become NULL, and the average uses the rest (stays) | 3,167 zero fares become NULL, and the average uses the rest. | ends on the measurement |

# Animation: SCD Type 2 closes one version and opens the next, and a late event goes into its place

**Claim.** Auto CDC applies events by sequence number. In SCD Type 2 a new value closes the current version; a late event (seq 4) goes between Toronto and Hanoi, while SCD Type 1 ignores it (Chapter 61, Steps 6 and 7).

**Numbers.** `data.json`, from `code/ch61/output/drops.txt` (c-01 after drop 1 and drop 2).

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | title and the two empty tables | SCD Type 2: close one version, open the next (stays) | Two targets read the same events. | subject at frame 0 |
| `e1` | 1.0 | Toronto seq 1 fills both tables | Each new value closes the current version | Each new value closes the current version. | the first version |
| `e2` | 3.0 | Hanoi seq 5: SCD1 overwrites, SCD2 closes Toronto at 5 and opens Hanoi | Each new value closes the current version | Hanoi closes Toronto at five. | the close and open |
| `e3` | 5.6 | Ottawa seq 4 arrives late: SCD1 no change, SCD2 closes Toronto at 4 and puts Ottawa between | A late event goes into its place by seq (stays) | A late event goes into its place by sequence. | the late event is the point of the chapter |

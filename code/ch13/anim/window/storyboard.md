# Animation: a window function is a shuffle, a sort and a Window operator

**Claim.** Spark runs a window as Exchange by the partition key, Sort by the order key, then Window, which walks each partition. From Chapter 13, Step 12.

**Numbers.** The 8-row sales table and the running sums from `code/ch13/output/windows.txt` (section 6, `running`).

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | title, scanned rows | How Spark runs a window function (stays) | How Spark runs a window function. | topic at frame 0 |
| `shuffle` | 1.4 | rows move to their borough partition | Exchange: a shuffle sends each borough to one partition | Exchange: a shuffle sends each borough to one partition. | the shuffle |
| `sort` | 4.8 | rows reorder by day | Sort: each partition orders its rows by day | Sort: each partition orders its rows by day. | the sort |
| `window` | 7.8 | a frame grows down; running sums appear | Window: the frame grows row by row and gives a running sum | Window: the frame grows row by row and gives a running sum. | the operator |
| `cost` | 12.0 | holds | One shuffle and one sort for each window specification (stays) | One shuffle and one sort for each window specification. | the rule |

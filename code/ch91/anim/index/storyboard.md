# Animation: the three default index types number the rows in three different ways

**Claim.** `sequence` moves every row to one partition to number it, `distributed-sequence` counts each partition first and then numbers all partitions in parallel, `distributed` numbers in parallel at once but leaves gaps. From Chapter 91, Step 2.

**Numbers.** 1.57 s, 0.96 s and 0.15 s to write all 3,837,248 rows with each index, `code/ch91/output/internals.txt` (median of 5 after a warmup). Four partitions of three rows each are schematic, labelled on screen.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | title, three lanes of four partitions | pandas needs a number for every row | pandas needs a number for every row. | topic at frame 0 |
| `sequence` | 1.4 | top lane: all rows move into one partition, then get 0 to 11 | sequence moves every row into one partition | sequence moves every row into one partition. | the costly mechanism |
| `dseq` | 4.6 | middle lane: counts appear, then offsets, then numbers in parallel | distributed-sequence counts each partition, then numbers in parallel | distributed-sequence counts each partition, then numbers in parallel. | the default |
| `dist` | 7.8 | bottom lane: numbers appear at once, with large gaps | distributed is fastest, but its numbers have gaps | distributed is fastest, but its numbers have gaps. | the trade |
| `cost` | 10.2 | measured times beside each lane | Time to number all 3.8 million rows (stays) | Time to number all 3.8 million rows. | ends on measurement |

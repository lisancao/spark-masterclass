# Animation: AQE merges the 200 small shuffle partitions of a groupBy into one task

**Claim.** The static plan asks for 200 reduce partitions. After the map stage, AQE reads the real sizes and coalesces adjacent partitions into one read. From Chapter 40, Step 6.

**Numbers.** `data.json`, from `code/ch40/output/aqe.txt` section 1 (16 map tasks, 200 partitions, 1 task, 258 rows). Partition sizes are schematic and labelled.

**Backend:** hf. **Themes:** paper. **Formats:** landscape (book figure, 1280x720 after scale). **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | title and sixteen map tasks | Sixteen tasks scan the trips and count per zone | Sixteen tasks scan the trips and count per zone. | the reader sees the map side first |
| `write` | 4.8 | 200 partition cells fill in | The shuffle writes 200 partitions, and most are almost empty | The shuffle writes 200 partitions, and most are almost empty. | the waste that AQE fixes |
| `plan` | 9.6 | ghost reduce tasks of the static plan | AQE reads the real sizes before the next stage starts | AQE reads the real sizes before the next stage starts. | shows what the static plan would do |
| `merge` | 14.4 | cells fly into one task box | Partitions next to each other merge: one task, not 200 (stays) | Adjacent partitions merge into one read: one task, not 200. | the result, with the plan node name |

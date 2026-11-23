# Animation: one event dated a day ahead moves the watermark, and each later event is late

**Claim.** Without a future check, one event from a device whose clock is a day ahead moves the watermark to 10-06 09:12, and the operators drop all rows of the later drops. From Chapter 49, Step 5.

**Numbers.** `data.json`, from `code/ch49/output/m4_naive.txt` and `m4_defended.txt`: watermark 09:02 after drop 1, 10-06 09:12 after drop 2, 10 of 10 late in drops 3 and 4, 5 sessions naive and 7 defended. Event positions inside each drop and the timing are schematic.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | drop 1 lands, watermark appears | Drop 1 sets the watermark to 09:02 | Drop 1 sets the watermark to 09:02. | normal behavior first |
| `future` | 4.8 | drop 2, e903 lands a day ahead, watermark jumps | One device clock is a day ahead, and the watermark jumps | One device clock is a day ahead, and the watermark jumps. | the cause |
| `late` | 9.6 | drops 3 and 4 turn late and fall | Each later event is older than the watermark, and Spark drops it | Each later event is older than the watermark, and Spark drops it. | the damage |
| `result` | 14.4 | table naive vs defended | Reject future events before withWatermark (stays) | Reject future events before withWatermark. | the fix, with measured results |

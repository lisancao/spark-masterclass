# Animation: the watermark follows the newest pickup 15 minutes behind, and each window goes out when it passes the end

**Claim.** In append mode with a 15-minute watermark, Spark writes a window when the watermark passes its end, and keeps only the open windows in state. From Chapter 48, Step 6.

**Numbers.** `data.json`, parsed from `code/ch48/output/s6_watermark.txt`: per batch the file, max pickup, watermark, windows out and state rows. The time between batches is schematic.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | axis, nine open windows, batch 0 | Each batch reads one file: trips that ended in five minutes | Each batch reads one file: trips that ended in five minutes. | the input |
| `watermark` | 4.8 | watermark appears, six windows rise | The watermark is the newest pickup minus 15 minutes | The watermark is the newest pickup minus 15 minutes. | the rule |
| `out` | 9.6 | one window goes out each second batch | A window goes out when the watermark passes its end | A window goes out when the watermark passes its end. | the effect |
| `nodata` | 14.4 | batch 12 writes the last window | A no-data batch writes the last window; state stays small (stays) | A no-data batch writes the last window, and the state stays small. | ends on the measured end state |

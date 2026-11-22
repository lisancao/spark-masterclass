# Animation: footer statistics and row-group statistics skip all data but one row group

**Claim.** On a layout sorted by user_id with small row groups, the reader uses footer min/max to skip files and row groups, and returns one row group of 15,360 rows of 2,000,000. From Chapter 36, Step 4.

**Numbers.** `data.json`, from `code/ch36/output/skipping.txt` (layout C: 8 files, 2,000,000 rows, 15,360 rows returned for `user_id = 16073`, 17 row groups in a file). The user_id ranges of files and row groups are schematic (20,000 users split evenly) and labelled.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | pushed filter, eight files | The filter user_id = 16073 goes to the Parquet reader | The filter user_id = 16073 goes to the Parquet reader. | the input to skipping |
| `files` | 4.8 | seven files dim | The footer min and max rule out seven of the eight files | The footer min and max rule out seven of the eight files. | file-level skip |
| `groups` | 9.6 | 17 row groups appear, 16 dim | Narrow row-group ranges let the reader skip all but one | Each row group has a narrow range, so all but one are skipped. | row-group skip |
| `result` | 14.4 | measured rows returned | The reader decodes one row group, and the Filter removes the rest (stays) | The reader decodes one row group, and the Filter removes the rest. | ends on the measured count |

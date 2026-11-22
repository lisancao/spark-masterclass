# Animation: the CSV reader fills columns by position from one header; unionByName matches by name

**Claim.** For a folder whose columns move, Spark takes one header and puts the values of every file into columns by position, so day 2 dates land in `id`; reading each header and `unionByName` fixes it. From Chapter 12, Step 18.

**Numbers.** Headers and the day 2 row (`2026-10-02`, `6`, `12.00`) from `code/ch12/output/csv_drift.txt`.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | title, three files and headers | One header for a folder whose columns move (stays) | One header for a folder whose columns move. | topic at frame 0 |
| `header` | 1.0 | day 3 header highlighted; column heads appear | Spark takes the column names from one file: day 3 | Spark takes the column names from one file: day 3. | the cause |
| `position` | 3.8 | the day 2 values move into columns by position | The day 2 values go into the columns by position | The day 2 values go into the columns by position. | the silent error |
| `byname` | 7.8 | the same values appear under their own names | unionByName reads each header and matches the columns by name | unionByName reads each header and matches the columns by name. | the fix |
| `end` | 11.4 | holds | Each file keeps its own header. All 13 rows are correct. (stays) | Each file keeps its own header. All 13 rows are correct. | the result |

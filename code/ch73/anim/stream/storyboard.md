# Animation: one function runs as a batch query and as a stream, with the same answer

**Claim.** The same DataFrame function reads a folder once as a batch, or one new file per micro-batch as a stream, and gives the same result. Chapter 73, Step 3.

**Numbers.** File names, rows per micro-batch, top zone, state rows from `code/ch73/output/same_code.txt` via `../build_data.py`. Timing schematic.

**Backend:** hf. **Themes:** ide. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | title, inbox, function | One function, a table or a stream (stays) | One function, a table or a stream. | topic at frame 0 |
| `batch` | 0.6 | three files pulse together, result appears | As a batch query, it reads the inbox once | As a batch, it reads the folder once. | the batch case |
| `stream` | 4.6 | one file per micro-batch, state pulses | As a stream, it reads one new file in each micro-batch | As a stream, it reads one new file at a time. | incremental |
| `same` | 10.2 | caption | Same function, same answer | Same function, same answer. | the claim |
| `newday` | 11.8 | fourth file, batch 3 | A new day lands: only the new file is read, state keeps 249 zones (stays) | A new file arrives, and only it is read. | ends on measured numbers |

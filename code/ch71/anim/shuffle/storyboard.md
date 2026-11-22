# Animation: a shuffle writes partitioned files, and each reduce task fetches one segment from each

**Claim.** Map tasks write one data file and one index each; every reduce task reads its partition from every map output, locally or over the network. Chapter 71, Step 6.

**Numbers.** Segment sizes, executors and local/remote bytes from `code/ch71/output/anatomy.txt` via `../build_data.py`. Timing is schematic.

**Backend:** hf. **Themes:** ide. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | title and two executors | One shuffle: write, files, fetch (stays) | One shuffle, three parts. | topic at frame 0 |
| `write` | 0.6 | four files fill segment by segment | Each map task writes its rows in reduce-partition order | Each map task writes its rows in partition order. | the write side |
| `index` | 4.8 | the offsets of one index file | One .index file per map output: where each partition starts | An index file says where each partition starts. | the file layout |
| `fetch` | 8.6 | segments fly to six reduce boxes | Each reduce task fetches its segment from every map output | Each reduce task fetches one segment from every file. | the read side |
| `local` | 12.6 | bytes under each reduce box | Blocks on its own executor are local, the others are remote (stays) | Some blocks are local, the rest come over the network. | ends on measured bytes |

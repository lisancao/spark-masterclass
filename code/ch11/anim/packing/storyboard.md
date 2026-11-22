# Animation: Spark packs small files into tasks with an open cost per file

**Claim.** Each file counts as its size plus `openCostInBytes`; with the default 4 MB, 2000 small files become 63 tasks; with 64 KB, the one-task-per-core minimum gives 16. From Chapter 11, Step 11.

**Numbers.** 63 and 16 tasks from `code/ch11/output/formats.txt`; 128 MB split and about 31 files per task from the chapter. Squares are schematic (one square for about 16 files), labelled on screen.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | title, file squares | How Spark packs 2000 small files into tasks (stays) | How Spark packs 2000 small files into tasks. | topic at frame 0 |
| `cost` | 0.9 | holds | Each file counts as its size plus a 4 MB open cost | Each file counts as its size plus a 4 MB open cost. | the rule |
| `pack` | 3.6 | squares fill 63 bins | A 128 MB split holds about 31 files, so 2000 files make 63 tasks | A 128 MB split holds about 31 files, so 2000 files make 63 tasks. | the default |
| `small` | 8.0 | squares move to 16 bins | With a 64 KB open cost, one task for each of the 16 cores | With a 64 KB open cost, one task for each of the 16 cores. | the contrast |
| `note` | 12.0 | holds | On an object store, each file open is a network request (stays) | On an object store, each file open is a network request. | why the default |

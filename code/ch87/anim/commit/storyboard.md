# Animation: algorithm 1 moves each file two times, the magic committer moves none

**Claim.** With FileOutputCommitter algorithm 1, each file is copied at task commit and again at job commit, and the job commit waits for the driver. The magic committer uploads each file to its final key and only completes the uploads at job commit. From Chapter 87, Steps 5 and 6.

**Numbers.** `data.json`, from `code/ch87/output/committers.txt`: 32 files, COPY 64 against 0, 32 multipart uploads, job commit 2.00 s against 0.21 s. Four files stand for the 32. Timing schematic.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `write` | 0.0 | four tasks write files into _temporary (top) and into __magic uploads (bottom) | Each task writes its files. Nothing is visible yet | Each task writes its files. Nothing is visible yet. | the start state |
| `task` | 4.0 | top: files copy to the job attempt folder. bottom: a .pendingset appears | At task commit, algorithm 1 copies each file. Magic saves a list | At task commit, algorithm 1 copies each file. Magic saves a list. | first difference |
| `job` | 8.5 | top: files copy again to the final folder. bottom: dashed files turn solid | At job commit, algorithm 1 copies again. Magic completes the uploads | At job commit, algorithm 1 copies again. Magic completes the uploads. | the second copy |
| `count` | 13.0 | counters | For 32 files: 64 copies and 2.00 s, against 0 copies and 0.21 s | For 32 files: 64 copies and 2.00 s, against no copies and 0.21 s. | the measured result |

# Animation: a delete hides the row; the bytes leave only after a rewrite and an expire

**Claim.** DELETE writes a delete file and hides the row; time travel still finds it; rewrite_data_files writes a new file but the old snapshot keeps the old one; expire_snapshots removes both. From Chapter 37, Step 15.

**Numbers.** rows with the hash 0; files that hold the text 1, 1, 1, 0 from `code/ch37/output/blobs.txt`.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | title, snapshot 1, data file A | A delete does not remove the bytes (stays) | A delete does not remove the bytes. | topic at frame 0 |
| `delete` | 1.0 | snapshot 2 and a delete file appear | DELETE writes a delete file. The row is hidden, the bytes stay. | DELETE writes a delete file. The row is hidden, the bytes stay. | the first surprise |
| `travel` | 4.8 | snapshot 1 and file A pulse | Time travel to snapshot 1 still finds the text | Time travel to snapshot 1 still finds the text. | why the bytes matter |
| `rewrite` | 7.2 | snapshot 3 and file B appear | rewrite_data_files writes a new file, but snapshot 1 keeps file A | rewrite_data_files writes a new file, but snapshot 1 keeps file A. | the second surprise |
| `expire` | 10.4 | old snapshots and file A go; count 0 | expire_snapshots removes the old snapshots and file A | expire_snapshots removes the old snapshots and file A. | the end state |
| `store` | 13.0 | holds | Then remove the blob from the store, with its hash (stays) | Then remove the blob from the store, with its hash. | the last step |

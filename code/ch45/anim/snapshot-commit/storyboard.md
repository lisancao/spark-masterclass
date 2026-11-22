# Animation: an Iceberg commit writes all new files first, then changes one pointer

**Claim.** An INSERT writes data files, a manifest, a manifest list and the next metadata file. No reader sees them until the catalog pointer changes from v1 to v2 in one step. From Chapter 45, Step 6.

**Numbers.** `data.json`, from `code/ch45/output/tables.txt`: v1 1,474 B, 30 data files, manifest 12,528 B, manifest list 4,499 B, v2 2,696 B, 3,837,231 rows. Timing schematic.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | pointer, v1, reader at 0 rows | Before the INSERT, the pointer names v1: an empty table | Before the INSERT, the pointer names v1: an empty table. | the start state |
| `files` | 4.8 | 30 data files, manifest, manifest list | The writer adds data files, a manifest and a manifest list | The writer adds data files, a manifest and a manifest list. | the files come first |
| `meta` | 9.6 | v2.metadata.json | Then it writes v2.metadata.json with the new snapshot | Then it writes v2.metadata.json with the new snapshot. | still invisible |
| `swap` | 14.4 | version-hint 1 to 2, pointer moves, reader sees rows | The commit is one change of the pointer: all files at once (stays) | The commit is one change of the pointer: all files at once. | the atomic step |

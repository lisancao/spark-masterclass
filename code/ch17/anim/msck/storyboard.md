# ch17-anim-msck

Claim: the metastore keeps its own list of partitions. Folders that another job writes or deletes do not change it; MSCK REPAIR TABLE (ADD by default) adds partitions for new folders, SYNC PARTITIONS also drops partitions with no folder.
Names and row counts: code/ch17/output/partitions.txt (sections 2 and 3). Timing is schematic.

| id | start | what moves | on-screen text | why |
|---|---|---|---|---|
| open | 0.0 | folders (left) and metastore partitions (right), three each | two lists of partitions | setup |
| drift | 1.8 | Bronx2 and Manhattan folders appear, right side unchanged, 113 rows | another job writes two folders | the gap |
| add | 4.6 | two rows slide into the metastore list, 159 rows | MSCK REPAIR TABLE adds them | ADD mode |
| gone | 7.4 | Bronx2 folder fades, metastore keeps it | a folder goes away | the other gap |
| sync | 10.0 | Bronx2 row leaves the metastore | SYNC PARTITIONS drops it | SYNC mode |

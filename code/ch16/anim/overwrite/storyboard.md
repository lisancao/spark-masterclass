# ch16-anim-overwrite

Claim: INSERT OVERWRITE with no PARTITION clause deletes all matching partitions first in STATIC mode; in DYNAMIC mode the tasks write into .spark-staging-{jobId} and the commit replaces only the partitions that got rows.
Folders and counts: code/ch16/output/partition_overwrite.txt (sections 1, 2 and 8). Order of operations: InsertIntoHadoopFsRelationCommand.scala and HadoopMapReduceCommitProtocol.scala at v4.3.0. Timing is schematic.

| id | start | what moves | on-screen text | why |
|---|---|---|---|---|
| open | 0.0 | two lanes, three day folders each | INSERT OVERWRITE ... June 2 again | the setup |
| static-del | 1.6 | STATIC lane: three folders turn red and fade | STATIC: delete the partitions first | the order |
| static-write | 4.0 | a new day=06-02 folder drops in | then the job writes June 2 | result |
| dyn-stage | 6.2 | DYNAMIC lane: a staging folder appears, a file moves into it | DYNAMIC: tasks write into .spark-staging | the order |
| dyn-commit | 9.0 | staging folder slides onto day=06-02, old one fades | the commit replaces only day=06-02 | result |
| end | 11.2 | counts: 1 of 3 against 3 of 3 | partitions after the run | consequence |

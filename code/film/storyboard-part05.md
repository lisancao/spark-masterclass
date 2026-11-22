# Storyboard: Part V chapter clips

Chapter format (1280x720, about 95x24 cells), embedded in the book's pages. VO lines are drafts, checked against the
scripts' output; the clips are muted in the book and the VO guide is for a narrated cut later.

## ch25.tape

| Beat | On screen | VO (draft) | Has to be readable |
|---|---|---|---|
| ticking | `s3_ticking.py`: files arrive, batches tick | A file arrives every four seconds. The query runs a micro-batch on the next trigger, and the state grows as new zones appear. | batch ids, rows, state rows |
| watermark | `s6_watermark.py` table | The watermark trails the newest pickup by fifteen minutes. Each window goes out when the watermark passes its end. | watermark column, windows out |
| crash | `s7_restart.py crash`, then the checkpoint listing | The driver dies after the sink writes batch three. The checkpoint has offsets for batch three, but no commit. | `offsets/3` with no `commits/3` |
| resume | `s7_restart.py resume` | The restart runs batch three again with the same input, and the counts come out the same. | batch 3 again, the counts |

## ch26.tape

| Beat | On screen | VO (draft) | Has to be readable |
|---|---|---|---|
| dead-letters | `m3_dead_letters.py` | Bad lines go to a dead-letter table with a reason. After a replay of batch three, the counts do not change. | the error column, `38 distinct` |
| future | `m4_watermark.py naive` | One event dated tomorrow moves the watermark a full day. Every later event is late. | `10-06 09:12`, the late counts |

## ch27.tape

| Beat | On screen | VO (draft) | Has to be readable |
|---|---|---|---|
| sessions | `t2_sessions.py` | Four drops become five batches. The last one has no input and fires the timers that the watermark expired. | `closed_by`, batch 4 with 0 rows |
| clock | `t3_time.py` | A timer fires five seconds after the last click. The page is kept eight seconds, then it is gone. | `timer` rows, `state before: None` |

## ch28.tape

| Beat | On screen | VO (draft) | Has to be readable |
|---|---|---|---|
| rules | `r1_rules.py` | Stateless, aggregation and deduplication run in Real-time Mode. Python UDFs, foreachBatch and the memory sink do not. | `ran`, the error conditions |
| latency | `r2_latency.py 200 20 mb_count rtm_count` | The same running count, Kafka to Kafka. Micro-batch answers in hundreds of milliseconds, Real-time Mode in tens. | the p50 and p99 columns |

## ch29.tape

| Beat | On screen | VO (draft) | Has to be readable |
|---|---|---|---|
| decode | `c1_hdfs.py`, the decoded deltas | Each delta file holds the keys that changed in one batch. A snapshot holds them all. | delta rows, the snapshot row |
| repartition | `c3_repartition.py` | Offline repartition writes one more batch with no input, and the state moves to four partitions. | the folder lists, the new batch |

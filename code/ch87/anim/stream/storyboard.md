# A stream over growing change logs

Claim: the casefile stream keeps a byte offset per log, takes whole lines up to 400 bytes per micro-batch, splits a log between batches when needed, and holds back a line without its newline.
Data: `data.json` = `../stream_plan.json`, the byte ranges that `casefile.ChangesStreamReader` plans on `code/data/messy/casefiles` with `maxBytesPerTrigger=400` and availableNow, run in plain Python. The rows per batch (7, 7, 8, 7, 7, 7), the total (43) and the end offset of batch 0 agree with `code/ch87/output/demo.txt`.

| beat | start | what moves | caption | why |
|---|---|---|---|---|
| logs | 0.4 | ten log bars | Each log is a bar of bytes. The offset is a byte per log. | the offset design |
| batches | 1.8 | batch 0 to 2 fill | Each micro-batch takes whole lines, up to 400 bytes. | admission control |
| split | 6.1 | batch 3 to 5 fill | It goes folder by folder, and splits a log between batches. | ranges across logs |
| held | 10.8 | dashed tail of CASE-0003 | A line without its newline waits for the next batch. | only complete lines |

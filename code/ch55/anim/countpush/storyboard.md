# A COUNT(*) that the source answers

Claim: Spark pushes filters first; when no filter remains, it offers COUNT(*), and jdocs replaces the scan, the aggregate and the shuffle with a LocalTableScan.
Data: `data.json`; the final plan and the counts come from `code/ch55/output/jdocs.txt`. The logical plan before pushdown is schematic.

| beat | start | what moves | caption | why |
|---|---|---|---|---|
| query | 0.4 | logical plan | The query counts the images in the folder. | the input |
| filter | 2.6 | Filter goes into the scan | Spark offers the filter first. jdocs takes it. | the order of pushdown |
| offer | 5.2 | supportCompletePushDown, pushAggregation | No filter remains above the scan, so Spark offers the COUNT(*). | the condition |
| local | 7.4 | Project + LocalTableScan, counts | jdocs answers from its file list. No scan, no shuffle. | the result |

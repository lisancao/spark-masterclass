# Two rules copy a Python UDF

Claim: PushDownPredicates replaces the alias in a filter with the UDF call, and ExtractPythonUDFs then makes one ArrowEvalPython per copy, so the UDF runs twice for the rows that pass.
Data: `data.json`, plan lines from the plan change log in `code/ch74/output/rules.txt` (shortened); counts from `code/ch74/output/twice.txt`.

| beat | start | what moves | caption | why |
|---|---|---|---|---|
| analyzed | 0.3 | plan before the rules | The filter refers to area, an alias of the UDF result. | the start |
| push | 3.8 | filter below the projection | To push the filter down, the rule copies the alias definition. | the copy |
| extract | 7.8 | two ArrowEvalPython nodes | Then each copy gets its own ArrowEvalPython operator. | two trips |
| count | 9.8 | 270 | Filter on 200 rows, project 70 rows: 270 calls in all. | the cost |

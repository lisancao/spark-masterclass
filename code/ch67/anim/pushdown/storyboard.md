# A filter pushed into a Python source

Claim: Spark offers simple filter objects; the source keeps what it applies and yields the rest; Spark keeps a Filter for what comes back.
Data: `data.json`, from `code/ch67/output/files_pushdown.txt` (the mixed query: PushedFilters, Spark's Filter, rows 3, opened 5 of 26).

| beat | start | what moves | caption | why |
|---|---|---|---|---|
| where | 0.4 | the WHERE clause | The optimizer splits the WHERE clause at each AND. | the input |
| offer | 2.0 | four filter chips | Spark offers four filter objects to pushFilters(). | what arrives |
| keep | 4.4 | EqualTo stays, three go back | The source keeps the kind filter and yields the other three. | the contract |
| filter | 6.4 | three chips in Spark's box | Spark keeps a Filter for each filter that comes back. | Spark checks only these |
| files | 8.4 | 26 squares, 5 opened, 3 rows | The tasks open only the images. Spark removes two more rows. | the result |

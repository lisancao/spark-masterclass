# Animation: a JSON read without a schema is two passes over the data

**Claim.** Without a schema, job 1 parses every record and merges a type per field (amount: double, then string); the action parses everything again. With a schema there is one pass. From Chapter 33, Steps 2 and 3.

**Numbers.** amount types from the chapter's day table and `code/ch33/output/infer.txt`; count() 0.94 s and 0.39 s from `infer_cost.txt` (median of 5). Dots are schematic, labelled on screen.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | title, three days, two jobs | JSON without a schema: two passes (stays) | JSON without a schema: two passes. | topic at frame 0 |
| `infer` | 0.9 | records stream into job 1; amount goes double to string | Job 1 parses every record and merges a type for each field | Job 1 parses every record and merges a type for each field. | the first pass |
| `second` | 5.2 | records stream into job 2 | Then the action parses all the records a second time | Then the action parses all the records a second time. | the second pass |
| `schema` | 8.8 | one lane, one job | With a schema that you write, there is no inference job | With a schema that you write, there is no inference job. | the fix |
| `cost` | 11.8 | measured times | On a million trips as JSON Lines (stays) | On a million trips as JSON Lines. | ends on numbers |

# Animation: shredding gives fields with one type their own typed Parquet column

**Claim.** On write, each field with one type gets a `typed_value` column; `amount`, with three kinds of value, stays binary; a typed extraction reads one column. From Chapter 15, Steps 11 and 13.

**Numbers.** Records from `code/data/messy/events` (days 2 and 3); column names from `code/ch15/output/shred.txt`; 5.40 s, 1.40 s and 0.31 s from `speed.txt` (median of 5).

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | title, three records, three columns | Shredding: typed columns inside a VARIANT (stays) | Shredding: typed columns inside a VARIANT. | topic at frame 0 |
| `write` | 1.2 | field values fly into their columns | The writer gives each field with one type its own typed column | The writer gives each field with one type its own typed column. | the mechanism |
| `mixed` | 5.8 | the binary column pulses | amount has three kinds of value, so it stays in a binary column | amount has three kinds of value, so it stays in a binary column. | the exception |
| `read` | 8.8 | the int64 column is boxed, others dim | try_variant_get(v, '$.user.id', 'bigint') reads one int64 column | A typed extraction reads one int64 column. | why it is fast |
| `cost` | 11.8 | measured times | avg(fare) on a million trips, with try_variant_get (stays) | On a million trips, the shredded column is as fast as a typed one. | ends on numbers |

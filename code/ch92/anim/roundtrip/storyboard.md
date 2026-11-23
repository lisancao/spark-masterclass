# Animation: a typed operation turns each binary row into a JVM object and back

**Claim.** A typed `filter` and `map` deserialize each row into a `Trip` object, run the lambda, and serialize the result. The untyped version evaluates expressions on the binary row. From Chapter 92, Steps 3 and 4.

**Numbers.** The first row `Trip(140,Some(1),Some(1),2.9,16.3,6.4)` and the times 0.21 s and 0.11 s, `code/ch92/output/datasets.txt` (median of 5 after a warmup). The slot boxes are schematic, labelled on screen.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | title, one binary row on each lane | One row, two ways to filter and compute | One row, two ways to filter and compute. | topic at frame 0 |
| `deser` | 1.4 | top: the row becomes a Trip object | A typed filter first builds a Trip object | A typed filter first builds a Trip object. | the cost |
| `lambda` | 4.0 | top: the lambda runs, a Tuple2 comes out, it becomes a row again | The lambda runs, then Spark encodes the result | The lambda runs, then Spark encodes the result. | the round trip |
| `expr` | 6.8 | bottom: the expression reads two slots in place | Untyped expressions read the binary row in place | Untyped expressions read the binary row in place. | the contrast |
| `cost` | 9.4 | measured times | Measured on 3.8 million trips (stays) | Measured on 3.8 million trips. | ends on measurement |

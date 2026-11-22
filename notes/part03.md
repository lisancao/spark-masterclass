# Part III notes: Files, formats and messy data

Chapters (old numbers in file names, unchanged by the renumber map): 11 `ch11-files-as-files.md`, 12 `ch12-text-logs-csv.md`, 13 `ch13-json.md`, 14 `ch14-complex-types.md`, 15 `ch15-variant.md`, 16 `ch16-xml-avro-protobuf.md`, 17 `ch17-documents-images-audio.md`. Code in `code/ch11` to `code/ch17`, outputs in `code/chNN/output/`, tapes in `code/film/ch11.tape` to `ch17.tape`, clip list in `book/media/clips/clips-part03.tsv`.

Cross-references to other parts use the final numbers of `book/roadmap.md`, as plain text (no link), because those files are not in this worktree and `mkdocs build --strict` fails on a missing target: Chapter 5, 18, 20, 22, 25, 30, 38, 61. The integrator can turn them into links.

## Technical names and verbs (STE)

Technical names: every API, function, option, configuration key, error condition, file format and operator name in code format (`binaryFile`, `_metadata`, `rowTag`, `singleVariantColumn`, `variant_set`, `from_avro`, `MERGE`, `Generate`, `ReadSchema`, `PushedFilters` and so on); format names (JSON, JSON Lines, CSV, XML, Avro, Protobuf, Parquet, Iceberg, Delta Lake, VARIANT, UTF-8, Latin-1); Kafka, Confluent, schema registry, tombstone, dead letter table, quarantine, corrupt record, descriptor set, wire format, magic byte, content-addressed store, row group, footer, shredding, struct, array, map, lambda, generator, higher-order function, UDF, UDTF, Arrow.

Technical verbs: read, write, parse, infer, shred, explode, cache, shuffle, split, trace, compile, encode, decode, hash, merge, commit, expire, rewrite, push (into the scan), prune.

Words allowlisted for the checker (`notes/.ste-allow-part03`): listing, setting(s), meaning, padding, encoding(s), string(s), leading, missing, pruning, tracing, ascending, descending, shredding, drifting, mapping, streaming, warning, finding, recording(s), addressing, embedding. Each is used as a technical name or a modifier, not as a verb form.

## STE checker results (final)

`scripts/ste-check book/chapters/chNN-*.md description --allow $(cat notes/.ste-allow-part03)`:

| Chapter | Findings |
|---|---|
| 11 | 0 |
| 12 | 0 |
| 13 | 0 |
| 14 | 0 |
| 15 | 0 |
| 16 | 0 |
| 17 | 0 |

Rules broken on purpose: none flagged by the checker. Code, plans, outputs and error names are verbatim and outside STE. A few sentences hold a long code identifier or a function signature in a table cell; the checker skips tables in part. The checker is a lint and cannot confirm the official dictionary; several approved-looking words (for example "provenance", "contract", "drift", "quarantine") are used as technical names.

## Environment

- `pyspark[connect]==4.3.0` (uv project in `code/`, Python 3.12, pandas, pyarrow 26.0.1), `local[*]` on 16 cores, `spark.sql.session.timeZone=UTC`, `SPARK_UI_PORT=4023`.
- `BOOK_DATA=$HOME/spark-masterclass/data` (taxi month June 2026). Scratch output in `data/work/` of the worktree (gitignored).
- Avro: `org.apache.spark:spark-avro_2.13:4.3.0`. Protobuf: `org.apache.spark:spark-protobuf_2.13:4.3.0`. Kafka: `org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0` (used only to print the source schema; no broker ran). All from Maven Central through `spark.jars.packages`.
- Iceberg (ch17 `blobs.py` only): no Iceberg release supports Spark 4.3 (1.12.0 ships runtimes up to 4.1). Used `$BOOK_DATA/jars/iceberg-spark-runtime-4.3_2.13-b1eab64.jar`, built from apache/iceberg PR #18749, commit b1eab64 (same jar as the ch31 writer; see `ch31-iceberg/notes/ch31.md`). Not a release; labelled in the chapter. Hadoop catalog under `data/work/ch17/blobs/warehouse`.
- Delta Lake: 4.4.1 (`delta-spark_4.2_2.13`) is built for Spark 4.2 and fails on 4.3.0 (verified by the part06 writer). Not used; the chapter says so.
- Timings: medians of 5 runs after one warm-up, each benchmark under `flock $HOME/spark-masterclass/data/.bench.lock`: `ch13/infer_cost.py`, `ch15/speed.py`. Film renders also under the lock.

## Coverage table

| Lesson | Topics | Where |
|---|---|---|
| LS03 data_sources | reader chain, Parquet pruning and pushdown, CSV options and modes, JSON Lines vs multiLine, JSON into VARIANT, ORC/Avro/XML, writer save modes, partitionBy, format comparison | ch11 Steps 1 to 11 (reader, writer, save modes, partition folders, five formats); ch12 CSV; ch13 Step 1; ch15 Step 5; ch16 Steps 1 and 9 to 10. Parquet internals: Part IV ch22. Python Data Source API: left out here, Part IX (ch50 onward) |
| LS04 external_sources_hof | complex types, explode/posexplode/collect, collect order, IGNORE/RESPECT NULLS, HOFs and tracing, aggregate zero trap, maps, ANSI access, VARIANT intro, explode vs HOF, max_by k | ch14 Steps 1 to 21; VARIANT in ch15. Window functions: Part II ch7 |
| MD05 files_as_files | binaryFile, globs, recursiveFileLookup, modifiedAfter, `_metadata`, content only when selected, maxLength, magic bytes, PNG header, sha2 dedup, limits | ch11 Steps 1 to 8, 11 |
| MD06 text_and_logs | text source, wholetext trap, regex, no-match table, timestamps, URLs, multi-line events, mixed text/JSON, quarantine, encodings | ch12 Steps 1 to 9 |
| MD07 csv_and_delimited | defaults, headers, enforceSchema, inferSchema cost, separators, quotes, unescapedQuoteHandling, multiLine, null/empty/NaN, whitespace, dates and locale, decimal commas, encodings, BOM, modes, DROPMALFORMED, layout drift, singleVariantColumn, from_csv, fixed width, writer defaults | ch12 Steps 10 to 20 |
| MD08 json_drift | drift taxonomy, inference, modes, corrupt column and pruning, strict schema, STRING + coalesce, books balance, explode vs outer, from_json, schema_of_json, json_tuple, get_json_object | ch13 Steps 1 to 10, 13 |
| MD09 complex_types | everything in the inventory, including NULL struct, withField/dropFields, dot paths, array NULL table, ANSI indexing, generators and the Filter before Generate, lambda tracing and its errors, aggregate zero trap, three-valued exists/forall, comparator, struct trick, zip, maps and dedup policy, ordered collect, to_json round trip, flatten, nested pruning | ch14 Steps 1 to 21 |
| MD10 variant_basics | parse_json, paths, casts, try_variant_get, schema_of_variant(_agg), singleVariantColumn with corrupt column, option-order trap, variant_explode(_outer), key/type profile, normalize, rules of the type, missing vs JSON null, decimals | ch15 Steps 1 to 8 |
| MD11 xml | rowTag, attributes and `_VALUE`, valueTag, excludeAttribute, inferSchema, one-or-many in struct and VARIANT modes, CASE normalization, from_xml, schema_of_xml, malformed XML and the legacy parser, namespaces, splitting, writer | ch16 Steps 1 to 8. `rowValidationXSDPath`: left out (no XSD in the sample data) |
| MD12 avro_and_protobuf | packages and errors, Avro container header, type mapping, unions, codecs, avroSchema on write, mixed folder and mergeSchema, resolution rules, byte walk, wrong-schema outcomes, modes, Confluent framing, classification, current state, Protobuf descriptors, wire format, proto3 defaults, options, PERMISSIVE asymmetry, encode errors, Confluent message index | ch16 Steps 9 to 22. Window primer: Part II ch7. Protobuf `recursive.fields.max.depth`, `Any`, wrapper types: left out (the sample schema has none) |
| MD13 documents_to_rows | row grain and provenance, binaryFile, e-mail UDF, Arrow vs pickle UDF plans, timezone of collect, forgiving parser, PDF UDTF, lateralJoin and .outer(), SQL LATERAL, left lateral trap, Markdown with built-ins, union with typed NULL, sha2 duplicates | ch17 Steps 1 to 5. UDF creation timing before a session: left to Part IV ch20 |
| MD14 images_and_audio | image source, invalid rows and dropInvalid, BGR order, origin vs path, arrow_udf, WAV with wave, mapInArrow and the int32 mismatch, streaming binaryFile needs a schema | ch17 Steps 6 to 11. Streaming preview: one sentence, Part V ch25 |
| MD31 variant_shredding | encoding, unshredded layout, shredding, footer inspection, configs, inference rules (4096 rows / 64 MB, 10%, scalar agreement, width/depth), per-file schemas, try_variant_get vs `::` pushdown, FAILED_READ_FILE cause, benchmark, promote fields | ch15 Steps 10 to 13 and "When to promote" |
| MD47 blob_table_design | inline vs pointer measurements, 64 KiB threshold, content-addressed store, sources/blobs/derived, MERGE traps, derived by (hash, version), deletes keep bytes, purge sequence, forget a file | ch17 Steps 12 to 15, on Iceberg (PR build). Delta-specific parts (DESCRIBE DETAIL, column mapping, liquid clustering, OPTIMIZE, autoCompact, REORG PURGE run): left out because no Delta release runs on Spark 4.3; REORG/VACUUM named in one sentence |
| WN02 variant | why not string/struct, parse_json vs try, to_variant_object, extraction forms, path syntax, INVALID_VARIANT_CAST, schema functions, variant_explode, no equality/grouping, JSON null, shredding | ch15 (table "Three ways", Steps 1 to 12) |

New in 4.3, checked against v4.3.0 source: archive reader (ch11, ch12, ch17), `JSON_TABLE` and `explodeEmbeddedArray` (ch13), codegen for array higher-order functions SPARK-37019 (ch14), `COUNT(DISTINCT)` map normalization SPARK-58420 (ch14), UDF transpilation (ch14, experimental conf), VARIANT edit functions SPARK-57804/57641/57417/58061/58062/58221/58222/58035 (ch15), variant extraction pushdown through aggregates SPARK-58089 (ch15), `AVRO_DATA_SOURCE_NOT_ENABLED` condition (ch16, observed).

## Claim-to-source table (chapters 13 to 17, new or changed in this pass)

| Claim | Source |
|---|---|
| ch13: inference 0.60 s, count with schema 0.39 s, sum 1.19 s vs 0.42 s, samplingRatio 0.19 s, 450.6 MB input | `code/ch13/output/infer_cost.txt` |
| ch13: samplingRatio still reads all lines, parses the sample | `JsonUtils.sample` (v4.3.0 `sql/core/.../json/JsonUtils.scala`) uses `Dataset.sample` |
| ch14: every numbered output | `code/ch14/output/nested.txt`, `lambdas.txt` |
| ch14: array HOF codegen in 4.3; ArraySort, MapFilter, TransformKeys/Values, MapZipWith, ZipWith keep CodegenFallback | SPARK-37019 commit ca9ae8cc51f; `higherOrderFunctions.scala` at v4.3.0; `lambdas.txt` codegen section |
| ch14: transpilation conf, ANSI requirement, plain UDF only | `SQLConf.scala` `spark.sql.experimental.optimizer.transpilePyUDFs` (v4.3.0); `python/pyspark/sql/udf.py` (`evalType == SQL_BATCHED_UDF`) |
| ch14: COUNT(DISTINCT) map normalization | SPARK-58420 commit 6f05301b273; `nested.txt` |
| ch14: PySpark traces lambdas in `_create_lambda`, `_invoke_higher_order_function` | `python/pyspark/sql/functions/builtin.py` v4.3.0 |
| ch15: all outputs | `code/ch15/output/land.txt`, `edit.txt`, `shred.txt`, `speed.txt` |
| ch15: edit function semantics, `create_if_missing` must be constant, `variant_strip_nulls` default include_arrays true, `is_valid_variant` since 4.2.0 | `variantExpressions.scala` v4.3.0 ExpressionDescription blocks |
| ch15: shredding sample 4096 rows / 64 MB | `ParquetOutputWriterWithVariantShredding.scala` (`maxBufferRows = 4096`, `maxBufferSize = 64 MB`) |
| ch15: 10% threshold | `InferVariantShreddingSchema.scala` (`minCardinality = (numNonNullVariants + 9) / 10`); 9% vs 11% test in `shred.txt` |
| ch15: maxSchemaWidth 300, maxSchemaDepth 50, writeShredding default true, all internal | `SQLConf.scala` v4.3.0 |
| ch15: pullOutExtractions (4.3, default true) | `SQLConf.scala` v4.3.0; SPARK-58089 commit 4ab9e91c5f8; `shred.txt` |
| ch15: speed table (typed 0.33 s, JSON 5.40/5.19 s, unshredded 1.15/1.40 s, shredded `::` 2.85 s, shredded typed 0.31 s; sizes) | `code/ch15/output/speed.txt` |
| ch16: XML multiLine default true, MultiLineXmlDataSource not splittable | `XmlOptions.scala` line 194, `XmlDataSource.scala` v4.3.0; `xml.txt` |
| ch16: legacy XML parser conf internal, since 4.1.0 | `SQLConf.scala` `spark.sql.legacy.useLegacyXMLParser` |
| ch16: Avro scan splittable | `connector/avro/.../AvroScan.scala` `isSplitable = true` |
| ch16: all Avro, Protobuf, missing-package outputs | `code/ch16/output/avro.txt`, `protobuf.txt`, `missing.txt`, `xml.txt` |
| ch17: all outputs | `code/ch17/output/documents.txt`, `media.txt`, `blobs.txt` |
| ch17: Arrow UDF/UDTF default since 4.2 | SparkTutor MD13 inventory (SPARK-54555); plan nodes `ArrowEvalPython`, `ArrowEvalPythonUDTF` in `documents.txt` |

Chapters 11 and 12 were written and committed by the earlier writer (commits 4d86529, 196c237); their claims trace to `code/ch11/output/*` and `code/ch12/output/*`.

## Open findings (possible Spark bugs or doc gaps; none filed)

1. **XML: one malformed element makes the default parser lose valid rows (4.3.0).** Repro: `code/ch16/xml.py` section "malformed XML". Five `<order>` elements, the second with `</custmer>`. Default parser: 2 rows (order 1 and one corrupt record holding the file from the start), orders 3 and 4 lost with no error. `spark.sql.legacy.useLegacyXMLParser=true`: 4 rows, only order 2 corrupt. SparkTutor saw the same on 4.2.0. Candidate for the `spark-bug` workflow.
2. **PySpark `lateralJoin(..., how="left")` without `on` drops rows whose UDTF gives no output (4.3.0).** Repro: `code/ch17/documents.py` section "a UDTF that gives no rows": 2 of 3 files; with `on=F.lit(True)` 3 of 3; SQL `LEFT JOIN LATERAL ... ON true` 3 of 3. Either a bug or an undocumented requirement.
3. **A transpiled Python UDF is still refused inside a higher-order function lambda (4.3.0).** With `spark.sql.experimental.optimizer.transpilePyUDFs=true`, `useArrow=False` and ANSI on, `py_triple` becomes `cast(id as int) * 3` in a plain select, but `transform(xs, x -> py_triple(x))` fails with `UNSUPPORTED_FEATURE.LAMBDA_FUNCTION_WITH_PYTHON_UDF`. Repro: `code/ch14/lambdas.py`. Probably a limitation of the experimental feature rather than a bug; worth a doc note.
4. **`mapInArrow` int64 for a declared `int` column gives `UNSUPPORTED_CALL.WITHOUT_SUGGESTION` with no column or type in the message (4.3.0).** Repro: `code/ch17/media.py`. SparkTutor recorded SPARK-56999 as open for this; status not re-checked against JIRA.
5. **Avro reader silently uses one file's schema for a mixed-version folder, and `mergeSchema` has no effect, with no warning (4.3.0).** Repro: `code/ch16/avro.py` "a folder with two versions of the schema". Documented behaviour may be intended; a warning would help.
6. **Classic PySpark: `.schema()` before `.option("singleVariantColumn", ...)` fails with `INVALID_SINGLE_VARIANT_COLUMN` (4.3.0).** Repro: `code/ch15/land.py`. Still present from 4.2.0.

## Not verified

- Clip pacing at 1x has not been watched by a person.
- The JIRA status of SPARK-56999 (finding 4).
- Shredding behaviour for files written by other engines; nanosecond timestamps inside VARIANT (not tested; the chapter makes no claim).
- Real PDFs (only the generated sample PDFs; the chapter says a library such as `pypdf` is needed and does not run it).
- The Kafka source against a running broker (schema only).

## Nav entries

```yaml
  - "Part III: Files, formats and messy data":
    - chapters/ch11-files-as-files.md
    - chapters/ch12-text-logs-csv.md
    - chapters/ch13-json.md
    - chapters/ch14-complex-types.md
    - chapters/ch15-variant.md
    - chapters/ch16-xml-avro-protobuf.md
    - chapters/ch17-documents-images-audio.md
```

## Clips

Rendered with `film render code/film/chNN.tape --format chapter` under the bench lock; each beat is one clip, copied from `code/film/renders/chapter/NN-<beat>.mp4` to `book/media/clips/<beat>.mp4`, with a `.webp` poster. The list is in `book/media/clips/clips-part03.tsv` (source = the per-beat render, so start 0 and end = its length).

| Clip | Chapter | Shows |
|---|---|---|
| ch11-bytes | 11 | `bytes.py`: a metadata query reads no content, `maxLength` on a content read |
| ch11-archives | 11 | `archives.py`: the archive reader off and on, `archivePathFilter` |
| ch12-quarantine | 12 | `logs.py`: parsed + rejects = input |
| ch12-decimal | 12 | `csv_options.py`: decimal commas with two locales |
| ch12-modes | 12 | `csv_drift.py`: the corrupt record column and `DROPMALFORMED` |
| ch13-quarantine | 13 | `drift.py`: the books balance, file by file |
| ch13-json-table | 13 | `functions.py`: `JSON_TABLE` on the log lines (4.3) |
| ch14-lambda | 14 | `lambdas.py`: the lambda runs once with a placeholder Column |
| ch14-codegen | 14 | `lambdas.py`: which higher-order functions get generated code (4.3) |
| ch15-edit | 15 | `edit.py`: the VARIANT edit functions (4.3) |
| ch16-kafka | 16 | `protobuf.py`: the Confluent header with a message index, dead letters |
| ch17-forget | 17 | `blobs.py`: a delete hides the row; rewrite and expire remove the bytes |

### Storyboard (drafts, for a narrated cut later)

| Beat | On screen | VO (draft) |
|---|---|---|
| ch11-bytes | `bytes.py`, then its first section | A query on file names and sizes reads no bytes. Only a query that selects content reads the files, and then the size limit applies. |
| ch11-archives | `archives.py`, archive reader off then on | With the reader off, a zip is binary noise. With it on, Spark reads each entry like a file in a folder. |
| ch12-quarantine | `logs.py`, the quarantine section | Each line is parsed or rejected, and the two counts add up to the input. |
| ch12-decimal | `csv_options.py`, decimal commas | With the wrong locale, twelve fifty becomes one thousand two hundred fifty, with no error. |
| ch12-modes | `csv_drift.py`, modes | The corrupt column keeps each bad row. DROPMALFORMED gives a different count for each query. |
| ch13-quarantine | `drift.py`, the books | For each file, the clean rows and the quarantine add up to what was read. |
| ch13-json-table | `functions.py`, JSON_TABLE | JSON_TABLE turns each JSON log line into a typed row, in SQL. |
| ch14-lambda | `lambdas.py`, the counting lambda | The Python function runs one time, on the driver, with a placeholder column. |
| ch14-codegen | `lambdas.py`, the codegen table | In Spark 4.3, transform, filter, exists, forall and aggregate compile to a Java loop. |
| ch15-edit | `edit.py`, the edit functions | Spark 4.3 can set, insert, delete and append inside a VARIANT value. |
| ch16-kafka | `protobuf.py`, the framing | A Confluent message has a magic byte, a schema ID and, for Protobuf, a message index before the data. |
| ch17-forget | `blobs.py`, the delete sequence | The delete hides the row at once. The bytes leave the disk only after a rewrite and an expire. |

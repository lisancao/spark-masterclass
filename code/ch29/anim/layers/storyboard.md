# Animation: a hand-written client turns DataFrame calls into a Relation tree, bytes, and back into a table

**Claim.** A Connect client is a builder of protobuf messages, a gRPC call and an Arrow reader. Each DataFrame call wraps the plan in one more Relation node. From Chapter 29, Steps 2 to 6.

**Numbers.** The node chain, the 320-byte plan and the five rows from `code/ch29/output/step4_dataframe.txt`. The timing is schematic.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | code panel with the DataFrame calls | A Spark client in a few hundred lines of Python | A Spark client in a few hundred lines of Python. | topic at frame 0 |
| `wrap` | 1.2 | one Relation node per call, each around the last | Each call wraps the plan in one more Relation | Each call wraps the plan in one more Relation. | the builder |
| `bytes` | 5.0 | the tree becomes a bar of bytes | protobuf turns the tree into 320 bytes | Protobuf turns the tree into 320 bytes. | serialization |
| `wire` | 7.2 | the bytes go to the server, Arrow comes back | gRPC sends the plan. Arrow batches come back. | gRPC sends the plan, and Arrow batches come back. | transport |
| `table` | 9.6 | the five rows | pyarrow reads them into a table: the same five rows | Pyarrow reads them into a table, with the same five rows. | the result |

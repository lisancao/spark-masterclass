# Animation: a Spark Connect query is a plan out and a stream of Arrow batches back

**Claim.** The client only builds an unresolved plan and reads Arrow. The server does all analysis and execution. From Chapter 80 (protocol.txt, captured with the logging proxy).

**Numbers.** `data.json`, copied from `code/ch80/output/protocol.txt`. Timing is schematic and labelled.

**Backend:** hf (boxes, packets, text). **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | title, client box, server box and executors on screen from frame 0 | One query, from a client to a server. | One query, from a client to a server. | the hook reads at frame 0 |
| `build` | 3.5 | plan nodes appear bottom-up in the client | The client builds a plan. It checks no names. | The client builds a plan. It checks no names. | the plan is unresolved |
| `send` | 8.3 | the nodes fold into one packet that moves along the gRPC line | The plan travels over gRPC as 695 bytes of protobuf. | The plan travels over gRPC as 695 bytes of protobuf. | protobuf over gRPC is the protocol |
| `analyze` | 13.9 | analyze, optimize, execute light up in turn, dots go to the executors | The server resolves names, optimizes the plan and runs it. | The server resolves names, optimizes the plan and runs it. | all Spark work is on the server |
| `stream` | 19.5 | response packets move back and list in the client | Results return as a stream of messages with Arrow batches. | Results return as a stream of messages with Arrow batches. | the response is a stream |
| `rows` | 25.9 | the three rows appear in the client (stays) | The client reads three rows from 496 bytes of Arrow. | The client reads three rows from 496 bytes of Arrow. | ends on measured numbers |

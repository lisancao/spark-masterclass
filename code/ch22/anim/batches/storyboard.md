# Animation: Arrow batches flow from Spark into Polars without a row conversion

**Claim.** Spark sends the result as Arrow record batches. The app hands the same column buffers to Polars, so no value goes through a row type. From Chapter 22, Steps 3 and 4.

**Numbers.** 6,145 rows, 4 columns, 145.4 KiB of Arrow IPC, from `code/ch22/output/app.txt`. The number of tasks and batches on screen is schematic.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | server with tasks, app with Polars | Arrow batches from Spark into Polars | Arrow batches from Spark into Polars. | topic at frame 0 |
| `tasks` | 1.2 | each task makes a batch of four columns | Each task writes its rows as an Arrow record batch | Each task writes its rows as an Arrow record batch. | where batches come from |
| `wire` | 4.0 | batches move through the gRPC stream | The batches stream to the app over gRPC | The batches stream to the app over gRPC. | the transport |
| `polars` | 7.0 | columns stack in the Polars frame | Polars takes the column buffers as they are | Polars takes the column buffers as they are. | no conversion |
| `end` | 10.0 | totals | 6,145 rows, 145.4 KiB of Arrow. No row conversion. | 6,145 rows and 145 KiB of Arrow, with no row conversion. | the result |

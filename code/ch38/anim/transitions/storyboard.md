# Animation: on the CPU, batches become rows above the scan; on an engine they stay batches to the top

**Claim.** Plain Spark puts its only transition right above the columnar scan, so every operator after it works on rows. With an engine that takes the whole plan, the only transition is at the top. From Chapter 38, Step 4.

**Numbers.** `data.json`: hourly query, CPU on 4.3.0 0 engine / 8 Spark operators / 1 transition, Gluten on 4.1.1 10 / 0 / 1 (`plans-cpu.txt`, `plans-gluten.txt`). The six-operator chain is simplified; timing schematic.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | two lanes, transition markers | The same query, and in each plan one transition | The same query, and in each plan one transition. | sets up the comparison |
| `cpu` | 4.8 | bars become dots right after the scan | On the CPU, batches become rows just above the scan | On the CPU, batches become rows just above the scan. | where the CPU transition sits |
| `engine` | 9.6 | bars travel to the top, then become dots | With Velox, batches stay batches until the result goes to the driver | With Velox, batches stay batches until the result goes to the driver. | where the engine transition sits |
| `read` | 15.2 | both markers pulse | Count the transitions in the plan before you time a query (stays) | Count the transitions in the plan before you time a query. | the takeaway |

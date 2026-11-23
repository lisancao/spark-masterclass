# Animation: one DataFrame query goes through the four plans of Catalyst, then codegen

**Claim.** Catalyst turns DataFrame calls into a parsed, analyzed, optimized and physical plan; codegen fuses a stage into one method. Chapter 3, Steps 3 and 5.

**Numbers.** None. Plan text from `code/ch03/output/rdd_vs_df.txt` and `codegen.txt` via `../build_data.py`.

**Backend:** hf. **Themes:** ide. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | title and phase strip | One DataFrame query through Catalyst (stays) | One query through Catalyst. | topic at frame 0 |
| `parsed` | 0.8 | parsed tree | The parser builds a tree from your DataFrame calls | The parser builds a tree. | start state |
| `analyzed` | 4.4 | analyzed tree | The analyzer resolves names and types against the schema | The analyzer resolves names and types. | resolution |
| `optimized` | 8.0 | optimized tree, new nodes in accent | The optimizer adds a Project and an isnotnull filter | The optimizer rewrites the tree. | the rewrite |
| `physical` | 11.6 | physical plan | The planner picks operators: two exchanges, a 2-column scan | The planner picks operators. | physical choice |
| `codegen` | 15.4 | the fused operators in a frame | Whole-stage codegen fuses the *(1) operators into one loop (stays) | Codegen fuses them into one loop. | ends on the codegen result |

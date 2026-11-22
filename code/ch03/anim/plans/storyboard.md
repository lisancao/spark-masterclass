# Animation: the DataFrame plan and the SQL plan become the same optimized plan

**Claim.** The analyzed plans differ (filter position, view and alias nodes); the optimizer removes the aliases and pushes the filter below the join, so `sameResult` is True. From Chapter 3, Step 4.

**Numbers.** Node names and the `sameResult: True` line from `code/ch03/output/quickstart.txt`. The trees show node names only, labelled on screen.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | title, then both analyzed trees | Two plans, one result (stays) | Two versions of the same query. | topic at frame 0 |
| `order` | 1.6 | the two Filter nodes pulse | The DataFrame filters before the join. SQL joins first. | The DataFrame filters before the join. SQL joins first. | shows why sameSemantics is False |
| `alias` | 4.4 | view and alias nodes fade, relations rise | The optimizer removes the view and alias nodes | The optimizer removes the view and alias nodes. | first rewrite |
| `push` | 7.2 | the SQL Filter moves below the Join | It pushes the filter below the join | It pushes the filter below the join. | second rewrite |
| `same` | 9.8 | sameResult: True appears between the trees | The optimized plans are the same (stays) | The optimized plans are the same. | the claim |

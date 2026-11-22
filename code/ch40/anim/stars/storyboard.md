# Animation: large-star and small-star turn a path into a star in three rounds

**Claim.** Each round changes edges, not labels; a path of 6 nodes is a star around node 1 after 3 rounds, and a path of 200 needs 8 rounds against 200 for label propagation (Chapter 40, Step 8).

**Numbers.** `data.json`, from `code/ch40/output/components.txt`. Node positions are schematic.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | path edges | Large-star and small-star: a path becomes a star (stays) | A path of six nodes. | subject at frame 0 |
| `rounds` | 2.4 | edges of rounds 1 to 3 replace each other, edge list on the right | Each round rewires edges toward the smallest node | Each round rewires edges toward the smallest node. | the convergence |
| `result` | 7.8 | final star stays | Path of 200 nodes: 8 rounds. Label propagation: 200. (stays) | On a path of two hundred, eight rounds against two hundred. | measured result |

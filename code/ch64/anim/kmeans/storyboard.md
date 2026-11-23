# Animation: KMeans moves its centers, iteration by iteration

**Claim.** Each iteration assigns each zone to its nearest center and moves each center to the mean of its zones. Most of the cost drop is in the first two iterations (Chapter 64, Step 2).

**Numbers.** `data.json`, from `code/ch64/output/kmeans_history.json` (written by `clustering.py`): the 128 zones on two scaled columns, and the centers and cost after maxIter = 1 to 8. The colour of each point is its nearest center, computed in the page from the saved centers. Timing is schematic.

**Backend:** hf. **Themes:** paper. **Formats:** landscape.

| beat | start | what moves | on-screen text |
|---|---|---|---|
| `open` | 0.0 | the 128 zones appear | KMeans on 128 zones: each iteration moves the centers |
| `start` | 1.4 | three centers appear | Three random starting centers |
| `iterate` | 3.0 | points take the colour of the nearest center, centers move; the cost label changes per iteration | Each row goes to its nearest center, then each center moves to the mean of its rows |
| `result` | 7.6 | the centers settle | Most of the drop is in the first two iterations |

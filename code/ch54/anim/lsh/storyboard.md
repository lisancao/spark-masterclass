# Animation: MinHashLSH puts sets into buckets, and compares only the sets in one bucket

**Claim.** Only sets that share a MinHash value become candidates; Spark computes the exact Jaccard distance for them. At 0.1 one pair matches, at 0.3 seven pairs (Chapter 54, Step 7).

**Numbers.** `data.json`, from `code/ch54/output/near_duplicates.txt`. The bucket of each message is schematic (one hash table of five) and says so on screen.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | messages and empty buckets | MinHashLSH: compare only what shares a bucket (stays) | Seven messages, and we want the near-duplicates. | subject at frame 0 |
| `buckets` | 1.0 | messages drop into buckets | Each set gets MinHash values, and equal values share a bucket | Equal MinHash values share a bucket. | the hashing step |
| `candidates` | 4.6 | lines between messages in one bucket, with exact distances | Spark computes the exact Jaccard distance only for candidates | Only candidates get the exact distance. | LSH chooses pairs, it does not estimate |
| `threshold` | 8.6 | all links shown | Threshold 0.1: 1 pair. Threshold 0.3: 7 pairs. (stays) | The threshold decides what a duplicate is. | measured result |

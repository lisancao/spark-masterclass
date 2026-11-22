# Animation: a theta sketch keeps the smallest hashes, and an intersection reads the shared ones

**Claim.** Theta keeps the smallest hashes up to a limit; the cut is theta; the intersection count divided by theta estimates the shared users (Chapter 57, Step 6).

**Numbers.** `data.json`, from `code/ch57/output/sketches.txt` (days 0 and 1: estimate 5,229, exact 5,175). The 30 users, hash positions and K = 12 are schematic and say so on screen.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | two hash lines | Theta sketch: users on day 0 and day 1 (stays) | Which users came on both days? | subject at frame 0 |
| `hash` | 0.5 | dots appear | Each user becomes a hash between 0 and 1 | Each user becomes a hash between zero and one. | the sample is uniform |
| `keep` | 3.6 | theta line, larger hashes dim | The sketch keeps only the smallest hashes. Theta is the cut | The sketch keeps only the smallest hashes. | the sketch |
| `intersect` | 7.2 | shared kept hashes link | Estimate = kept hashes on both days / theta | Count the shared ones and divide by theta. | the intersection |
| `result` | 10.0 | hold | Days 0 and 1: estimate 5,229, exact 5,175 (stays) | Five thousand two hundred, against five thousand one hundred. | measured result |

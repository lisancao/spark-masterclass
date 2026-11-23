# Animation: refresh selection decides which flows run and when a checkpoint folder starts again

**Claim.** --refresh runs only the selected flows; --full-refresh also starts a new checkpoint folder; --full-refresh-all does it for each dataset. Chapter 57, Step 8.

**Numbers.** From `code/ch57/output/refresh.txt` (EXCLUDED flows; folders 0 1 for raw_trips after its full refresh; 0 1 2 for both after --full-refresh-all, clean_trips got its folder 1 from an earlier `--full-refresh clean_trips` run that the animation does not show and the note says so).

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `refresh` | 1.0 | borough_daily orange, others EXCLUDED | Only the selected flow runs. The others are EXCLUDED. | same | the basic selection |
| `full` | 5.0 | raw_trips orange, folder label 0 -> 0 1 | A full refresh clears the table and starts a new checkpoint folder. | same | the checkpoint effect |
| `all` | 9.2 | all orange, folders 0 1 2 | --full-refresh-all does this for each dataset. | same | the whole graph |

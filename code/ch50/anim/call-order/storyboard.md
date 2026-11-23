# Animation: in one batch, handleInputRows runs before handleExpiredTimer, so u1's session closes by gap

**Claim.** Batch 2 runs with watermark 10:02:28, after u1's timer at 09:39:18, but its input has c018 at 10:45:56. Spark calls handleInputRows first, which closes the session by gap and moves the timer. From Chapter 50, Step 5.

**Numbers.** `data.json`, from `code/ch50/output/t2_sessions.txt`: session 09:00:14 to 09:09:18 with 3 clicks and 16.39 revenue, timer 09:39:18, watermark 10:02:28, c018 10:45:56. The new timer (11:15:56) follows the Sessionizer code. Timing schematic.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | session bar and timer | u1 has an open session and a timer at 09:39:18 | u1 has an open session and a timer at 09:39:18. | the state before the batch |
| `watermark` | 4.8 | watermark passes the timer; c018 lands | Batch 2: the timer expired, and a new click arrived | Batch 2: the timer expired, and a new click arrived. | both paths could fire |
| `input` | 9.6 | step 1 code, session closes, new timer | handleInputRows runs first, sees the gap, and closes the session | handleInputRows runs first, sees the gap, and closes the session. | the call order |
| `timer` | 14.4 | step 2 code | Then the timers run, so the session closed by gap, not by timer (stays) | Then the timers run, so the session closed by gap, not by timer. | explains the output row |

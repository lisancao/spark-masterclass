# Animation: a retry with the same Idempotency-Key gets the stored answer

**Claim.** After a timeout, the client does not know if the model ran; a retry with the same key gets the stored answer, so the model runs no extra time (Chapter 42, Step 8).

**Numbers.** `data.json`: the timeout and the slow answer from `code/ch42/service.py`, the retry counts from `code/ch42/output/calling.txt` (45 + 16 = 61 texts with a second or third try, 0 texts run twice). Motion speed is schematic.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | task, service, model | A safe retry: the same Idempotency-Key (stays) | One request, and a slow answer. | subject at frame 0 |
| `send` | 0.8 | POST with k1, the model runs | The task sends 10 items with the key k1 | The task sends ten items with a key. | the first try |
| `timeout` | 2.9 | client timeout, answer stored | The answer is slow, and the client does not know if the model ran | The answer is slow. Did the model run? | the uncertainty |
| `retry` | 5.8 | backoff, retry with k1, stored answer returns | The retry gets the stored answer. The model does not run again | The retry gets the stored answer. | the mechanism |
| `result` | 10.0 | hold | 61 texts needed a retry. Texts the model ran on twice: 0 (stays) | Sixty-one retries, and no text ran twice. | measured result |

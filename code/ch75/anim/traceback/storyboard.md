# Animation: a UDF error travels from the Python worker to your code

**Claim.** The worker sends the traceback text to the executor JVM, the task fails, and the driver raises a `PythonException` in your code. The settings change what the message holds (Chapter 75, Steps 5 and 6).

**Numbers.** `data.json`, from `code/ch75/output/tracebacks.txt` and `crash.txt`. Motion and timing are schematic.

**Backend:** hf. **Themes:** paper. **Formats:** landscape.

| beat | start | what moves | on-screen text |
|---|---|---|---|
| `open` | 0.0 | four boxes appear | A UDF error goes from the worker to your code |
| `travel` | 1.4 | the error chip moves box to box | The worker sends the traceback text, and the driver raises a PythonException |
| `settings` | 5.2 | four rows appear | default: 6 lines, 1 frame; full: 26 lines, 9 frames; with locals; faulthandler: 16 frames |

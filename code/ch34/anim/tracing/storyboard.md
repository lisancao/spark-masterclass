# Animation: a Python lambda in a higher-order function becomes a Catalyst expression

**Claim.** PySpark calls the lambda one time on the driver with a placeholder Column; `x * 2` builds an expression tree; executors loop in the JVM. From Chapter 34, Step 12.

**Numbers.** calls 1, `Column<'x_1'>`, result `[2, 4, 6, None]` from `code/ch34/output/lambdas.txt`.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | title, code | A Python lambda becomes a Catalyst expression (stays) | A Python lambda becomes a Catalyst expression. | topic at frame 0 |
| `call` | 1.0 | placeholder appears, calls goes to 1 | PySpark calls the function one time, with a placeholder Column | PySpark calls the function one time, with a placeholder Column. | the trace |
| `tree` | 4.6 | the expression tree builds up | x * 2 on a Column builds an expression, not a number | x * 2 on a Column builds an expression, not a number. | what the call returns |
| `run` | 7.8 | the tree goes to the executor; elements double | Executors run the loop in the JVM. No Python worker starts. | Executors run the loop in the JVM. No Python worker starts. | where the work runs |
| `sql` | 11.8 | holds | The SQL form transform(xs, x -> x * 2) gives the same tree (stays) | The SQL form gives the same tree. | the equivalence |

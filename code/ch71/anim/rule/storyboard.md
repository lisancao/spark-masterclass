# An optimizer rule replaces a Python UDF

Claim: PngRule matches a PythonUDF by name, determinism and types, and replaces it with Coalesce(IsPng(content), false), which keeps the UDF's False for NULL.
Data: `data.json`; plan lines from `code/ch71/output/planchange.txt`, values from `code/ch71/output/extensions.txt`.

| beat | start | what moves | caption | why |
|---|---|---|---|---|
| udf | 0.4 | plan line, expression tree | A plan calls the Python UDF is_png on the bytes of each file. | the input |
| checks | 2.6 | three checks | The rule checks the name, the determinism and the types. | when a rewrite is safe |
| rewrite | 5.0 | Coalesce(IsPng) | It puts the native IsPng in place, inside a Coalesce. | the change |
| values | 7.6 | three value lists | Coalesce keeps False for NULL, as the Python function does. | same results |
| end | 10.0 | hold | No Python UDF remains, so the plan needs no Python worker. | the gain |

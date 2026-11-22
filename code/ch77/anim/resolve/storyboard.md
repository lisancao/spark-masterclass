# Where does sales resolve?

Claim: on 4.3.0 the unqualified source name of a metric view follows the session namespace and reads the wrong table; with the fix it resolves in the creation namespace, like a regular view.
Data: `data.json`, results from `code/ch77/output/repro.txt` (4.3.0) and `test_fix.txt` (patched classes).

| beat | start | what moves | caption | why |
|---|---|---|---|---|
| setup | 0.4 | two namespaces, mv | A metric view in default names its source sales, with no namespace. | the setup |
| use | 2.2 | current namespace moves | The reader switches the session to the namespace other. | the trigger |
| bug | 4.0 | name to other.sales, (east, 999) | On 4.3.0 the name follows the session, and reads the wrong table. | the wrong result |
| fix | 7.0 | name to default.sales | With the fix, it resolves where the view was created, like a regular view. | the fix |

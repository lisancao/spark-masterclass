# Each check stops a different error

Claim: registration, the graph check, the run and a trusted number each stop a different version of the gold flow; only the trusted number stops the epoch error.
Data: `data.json`, outcomes from `code/ch78/output/dryrun.txt`.

| beat | start | what moves | caption | why |
|---|---|---|---|---|
| open | 0.4 | five flow files | An agent writes five versions of the same flow. | the inputs |
| register | 1.6 | gold_conf stops | Registration refuses a flow that changes the session. | first gate |
| resolve | 3.6 | typo and column stop | The graph check refuses a wrong table name and a wrong column. | second gate |
| run | 6.0 | two flows complete | Two flows run. Both complete without an error. | the silent pass |
| trusted | 8.6 | epoch stops, ok passes | Only a trusted number shows that one result is wrong. | the limit of the checks |

# A relation plugin on a Connect server

Claim: the client packs its own message into Relation.extension; the server planner asks each plugin in turn; PlanNodesPlugin matches the type URL, plans the input and returns rows; an unknown message fails with NO_HANDLER_FOR_EXTENSION.
Data: `data.json`, type URLs, rows and the error from `code/ch72/output/plugins.txt`.

| beat | start | what moves | caption | why |
|---|---|---|---|---|
| pack | 0.4 | message on the client | The client packs its own message into Relation.extension. | the extension point |
| send | 2.8 | message to the server | The server planner gets a relation of the type EXTENSION. | the planner path |
| match | 4.4 | type URL check | It asks each plugin in turn. PlanNodesPlugin knows the type URL. | dispatch by type URL |
| rows | 6.2 | rows back to the client | The plugin plans the input and returns its nodes as rows. | the result |
| unknown | 10.5 | error text | A message that no plugin knows fails on the server. | the failure case |

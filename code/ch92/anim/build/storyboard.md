# Building Spark from source

Claim: a build of the v4.3.0 tag is a Maven reactor of 41 modules in order; SQL, Core and Catalyst take the most time; make-distribution.sh then copies the jars into a distribution and packs it.
Data: `code/ch92/output/reactor.txt` (each module's time), `inspect.txt` (225 jars, 361M tgz). The clock runs at about 160 build seconds for each clip second.

| beat | start | what moves | caption |
|---|---|---|---|
| reactor | 0.5 | module tiles fill in reactor order, clock counts build time | Maven builds 41 modules in order. Each tile is one module. |
| big | 4.0 | SQL, Core, Catalyst tiles marked | SQL, Core and Catalyst take the most time. |
| dist | 9.0 | jars to dist/, tgz | make-distribution.sh copies 225 jars into dist/ and packs a 361M tgz. |

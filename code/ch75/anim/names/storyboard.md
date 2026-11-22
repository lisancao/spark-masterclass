# ch75-anim-names

Claim: a short table name is completed with the current catalog and namespace; USE changes them; a three-part name ignores them.
Names and results: code/ch75/output/catalogs.txt (sections 3, 5, 6).

| id | start | what moves | on-screen text | why |
|---|---|---|---|---|
| open | 0.0 | title, current = spark_catalog.default | A table name has three parts | hook |
| use | 1.4 | USE taxi, current namespace changes | USE sets the current namespace | the state |
| short | 3.6 | "zones" gains "spark_catalog.taxi." | Spark adds the current catalog and namespace | resolution |
| use2 | 6.6 | USE h2.ops, both parts change | USE can also change the catalog | catalogs |
| short2 | 8.4 | "zone_owner" gains "h2.ops." | The same short name now looks in H2 | consequence |
| full | 11.0 | the cross-catalog join, names unchanged | A three-part name works from anywhere | the safe form |

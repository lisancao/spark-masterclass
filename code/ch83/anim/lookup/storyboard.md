# How Spark finds a packaged source

Claim: a worker imports every top-level pyspark_* module and registers each DefaultSource; one module that fails to import makes Spark skip the whole lookup.
Data: `data.json`, outcomes of runs 2 and 4 in `code/ch83/output/packaging.txt`; the loop in `lookup_data_sources.py` at v4.3.0. The order of the modules on screen is alphabetical and schematic.

| beat | start | what moves | caption | why |
|---|---|---|---|---|
| scan | 0.4 | modules, skip or import | A worker imports each top-level module named pyspark_*. | the convention |
| register | 3.4 | registry entry, 4 rows | It registers the DefaultSource of each module under its name. | the result |
| broken | 6.0 | second package fails | Now a second package fails to import. | the trap |
| skip | 9.4 | warning, DATA_SOURCE_NOT_FOUND | Spark skips the whole lookup. No packaged source is registered. | one bad package hides all |

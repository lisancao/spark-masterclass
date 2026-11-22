# v2-geospatial notes: Chapter 76 (Geospatial data in Spark 4.3) and the Chapter 10 split

Branch `v2-geospatial`, worktree `~/spark-masterclass-parts/geospatial`. Ports: `SPARK_UI_PORT=4143`, Connect gRPC 15103 (UI 4144).

## Scope and changes

- **New:** `book/chapters/ch76-geospatial.md` (provisional number 76, for the new Spark SQL part). Code in `code/ch76/`, film tape `code/film/ch76.tape`, clips `book/media/clips/ch76-*`, cut list `book/media/clips/clips-ch76.tsv`, figures `book/media/figures/ch76-*.svg`, animation source `code/ch76/anim/bbox-prefilter/` (renders ignored).
- **Changed, Chapter 10:** Steps 8 to 15 (geospatial) moved into Chapter 76 (as Steps 2, 3, 10, 12, 13, 14). Chapter 10 is now "Time series": new title line, new opening paragraph and thesis (the geospatial sentences removed, a sentence on `ROWS` frames added), a pointer to Chapter 76 under the script list and before the Part II closing line, geospatial bullets and further-reading links removed. The file name stays `ch10-time-series-geospatial.md` so that links do not break; the integrator may rename it to `ch10-time-series.md`.
- **Moved code:** `code/ch10/geo.py` -> `code/ch76/geo_points.py`, `code/ch10/output/geo.txt` -> `code/ch76/output/geo_points.txt` (re-run on 4.3.0 after the move: output byte-identical). `code/film/ch10.tape` beat `nearest` now runs `ch76/geo_points.py`. The clip keeps its name `ch10-nearest.mp4`, and Chapter 76 Step 13 embeds it (the caption says it was filmed as `ch10/geo.py`). The integrator can rename it to `ch76-nearest` with the cut list.

## Lisa's premise ("Spark now has more geospatial functions"): not true for 4.3.0

Checked from source, not memory:

- `FunctionRegistry.scala` at `v4.3.0` (lines 1002-1006), `sql-expression-schema.md` at `v4.3.0`, `sql/api/.../functions.scala` and `python/pyspark/sql/functions/builtin.py`: the same five functions, `st_asbinary`, `st_geogfromwkb`, `st_geomfromwkb`, `st_setsrid`, `st_srid`. Identical at `v4.2.0`.
- `upstream/master` fetched 2026-11-22 (`3bb5201b836`): still the same five. No newer ST function on master. (Master has SPARK-8682 "range join for range-predicate joins", not in 4.3.0 and not spatial; not mentioned in the chapter.)
- What changed in the geo area between 4.2.0 and 4.3.0 (commits only in tag v4.3.0, `git tag --contains`): SPARK-60518 (no geo partition columns), SPARK-60519 and SPARK-60514 (Hive SerDe errors), SPARK-60470 (CatalystTypeConverters ordinal fix). The rest of the 4.2..4.3 geo commits are cherry-picks also in 4.2 (SPARK-56682 endianness, SPARK-56771 default on, SPARK-57058 BinaryView, SPARK-57766 WKB count validation, docs).
- 4.3.0 release notes (spark.apache.org/releases/spark-release-4-3-0.html, fetched 2026-11-22): no geospatial item.
- JIRA: umbrella SPARK-51658 "SPIP: Add geospatial types in Spark" (resolved, 4.2.0). 57 related issues, all resolved, fix versions 4.1.0/4.2.0 (and connect-swift). No issue for ST_Contains, ST_Distance, ST_X, ST_GeomFromText. Internal WKT writer exists (SPARK-55339) but no SQL function exposes it.
- Possible source of the impression: the Spark 4.2 docs page `sql-ref-geospatial-types.md` and SPARK-59226 ("Support geospatial ST functions", fix version connect-swift-0.8.0, which is the Swift client, not Spark).

The chapter says this plainly in the opening note and builds the missing operations with SQL.

## Technical names (TN) and technical verbs (TV)

- TN: GEOMETRY, GEOGRAPHY, SRID, CRS, WKB, WKT, GeoJSON, NDR, XDR, `POINT EMPTY`, ring, edge, polygon, multipolygon, line string, vertex, bounding box (bbox), grid, cell, grid key, ray, even-odd rule, shoelace formula, haversine formula, great circle, projection, Lambert conformal conic, standard parallel, State Plane, EPSG, OGC, PROJ, shapefile, `.shp`, `.dbf`, `.prj`, registry, logical type, geospatial statistics, column chunk, footer, format version, metadata table, lower bound, upper bound, service area, null island, validation ladder, Arrow cache serializer, `InMemoryRelation`, `BroadcastHashJoin`, `BroadcastNestedLoopJoin`, `Generate`, Citi Bike, TLC, PyArrow, pyproj, shapely, Apache Sedona, PostGIS, PR build.
- TV: parse, cast, project (a point onto a plane), explode, join, broadcast, cache, collect, validate, serialize, write, read, filter, group.
- `scripts/ste-check book/chapters/ch76-geospatial.md --allow bounding crossings`: **0 findings**. `bounding` (bounding box) and `crossings` (edge crossings) are technical names, never verbs.
- `scripts/ste-check book/chapters/ch10-time-series-geospatial.md --allow running sliding tumbling moving docstring`: **0 findings** (the same words Part II allowlists).
- Deliberate rule breaks: "Thus" starts sentences, as in Part I. Parentheses hold JIRA numbers, SRIDs and units. Admonition titles are commands in the STE caution form. Code-format identifiers make some list items long, but each sentence stays within 25 words.

## Claim-to-source

| Claim | Source |
|---|---|
| Five ST functions in SQL, PySpark, Scala; 591 functions | `output/inventory.txt` §1; FunctionRegistry v4.3.0 |
| Signatures, `srid` arg new in 4.2 (SPARK-55295), endianness new in 4.2 (SPARK-56682) | `output/inventory.txt` §2; JIRA fix versions; git log v4.1.0..v4.2.0 |
| `spark.sql.geospatial.enabled` internal, version 4.1.0, default true; default true since 4.2.0 (SPARK-56771) | `SQLConf.scala` v4.3.0 lines 757-763; commit 707012b3edb in v4.2.0 |
| Flag off gives `GEOSPATIAL_DISABLED` | `output/inventory.txt` §4 |
| PostGIS/Sedona names unresolved | `output/inventory.txt` §5 |
| Constructor result types, SRID labels | `output/geotypes.txt` §1 |
| Registry 10,713 entries, 1,406 geographic, from PROJ 9.8.1, SRID 0 added in code | `sql/api/src/main/resources/org/apache/spark/sql/srs_registry.csv` at v4.3.0 (header, row count); `SpatialReferenceSystemCache.java` |
| st_setsrid rules, invalid SRIDs | `output/geo_points.txt` §2 |
| Casts table | `output/geotypes.txt` §3 |
| DDL parse results incl. bare GEOMETRY error | `output/inventory.txt` §3 |
| PySpark knows only SRIDs 0, 3857, 4326; Classic and Connect fail on 2263 | `output/geotypes.txt` §6, `output/connect_srid.txt`; `pyspark/sql/geo_utils.py` 4.3.0 lines 57-61; same on upstream/master |
| WKB point layout, 21 bytes | `output/geo_points.txt` §1 |
| NDR/XDR | `output/geotypes.txt` §2 |
| Parse rules (NaN empty point, infinity, ranges, rings) | `output/geotypes.txt` §4; `docs/sql-ref-geospatial-types.md` v4.3.0 |
| Python objects, `GEO_ENCODER_SRID_MISMATCH_ERROR` | `output/geotypes.txt` §5 |
| Shapefile: 263 records, 354 rings, 98,192 vertices, 7,814 max, 23 multi-polygon zones, no holes, EPSG:2263 | `output/zones.txt` §1; `taxi_zones.prj` |
| Zone WKB sizes, edges, extents | `output/zones.txt` §2 |
| Borough areas (Manhattan 22.7 sq mi) | `output/zones.txt` §3 |
| Shoelace vs shapely 1.71e-10 | `output/check_reference.txt` |
| WKT/GeoJSON out, 240 single-ring zones | `output/zones.txt` §4 |
| GeoJSON in: 240/240 identical bytes, 74,870 largest | `output/zones.txt` §5 |
| WKB coordinate read-back | `output/zones.txt` §6 |
| 5,384,468 trips, 3,154 without coords, 2,279 distinct coordinates | `output/pip.txt` §1 |
| Projection equals pyproj (0.000000 ft max over 2,279 points) | `output/check_reference.txt` |
| January 2015 TLC yellow file has only `PULocationID`, no coordinates | downloaded `yellow_tripdata_2015-01.parquet` on 2026-11-22, schema read with PyArrow (file then deleted; not saved as output) |
| Pair counts 599,377 / 14,686 / 4,624; edge tests 222,972,802 / 2,979,104 / 1,066,168; zone cells 1,678, 475 cells, max 13 | `output/pip.txt` §2-3 |
| Grid plan | `output/pip.txt` §4 |
| Each trip in exactly one zone | `output/pip.txt` §5 |
| Spark PIP equals shapely on all 2,279 points | `output/check_reference.txt` |
| Timings (2.94 / 0.26 / 0.24 / 0.53 / 0.64 / 7.09 s) and join operators | `output/pip_bench.txt` (bench lock) |
| Trips per zone, borough totals, ratios (Brooklyn 14x, Manhattan near equal) | `output/pip.txt` §6, `output/trips_per_zone.csv` |
| Parquet logical types, geo stats on GEOMETRY only, bbox | `output/storage.txt` §1 |
| Parquet refusals; partition refusal only in v4.3.0 (SPARK-60518) | `output/storage.txt` §2; `git tag --contains 8ad5faba783` = v4.3.0 only |
| Hive SerDe refusal (SPARK-60519) | commit 215dda27a39, only in v4.3.0. Not tested in the chapter (said so). |
| Iceberg v3 schema strings, v2 and ANY refusals, file bounds | `output/storage.txt` §3 |
| Iceberg runtime build | `notes/ch31.md` (PR #18749, commit b1eab64) |
| ORDER BY/=/JOIN fail, GROUP BY/DISTINCT MatchError, max_by/collect_list work, cache fails, show() prints BinaryView, to_json fails | `output/limits.txt` |
| show() prints BinaryView also on 4.2.0 | manual run with `~/spark-masterclass/data/ch24/venv-4.2.0` (pyspark 4.2.0), 2026-11-22; not saved as output |
| Arrow cache serializer lists geo types, static conf | `docs/sql-arrow-cache-format.md` v4.3.0 lines 41-57, 184; `output/limits.txt` (--arrow-cache) |
| Haversine, nearest store, NEAREST BY | `output/geo_points.txt` §7-8 (moved from Chapter 10) |
| Validation ladder | `output/geo_points.txt` §4-6 |

## Environment

- Linux 7.0.0, 16 cores, OpenJDK 17, Python 3.12.3, `pyspark==4.3.0` and pyarrow 26.0.1 from `code/uv.lock`. Local mode `local[*]`, `spark.sql.shuffle.partitions=64` for `pip.py` and `pip_bench.py`.
- Reference check only (not book dependencies): `uv run --with shapely==2.1.2 --with pyproj==3.7.2` (PROJ 9.5.1 inside pyproj).
- Iceberg: `$BOOK_DATA/jars/iceberg-spark-runtime-4.3_2.13-b1eab64.jar` (PR #18749 build from Chapter 31), Hadoop catalog in `data/work/ch76/warehouse`.
- Connect: `code/ch76/connect_server.sh start|stop` (gRPC 15103, UI 4144, pid and logs under `$BOOK_DATA/pids-ch76`, `logs-ch76`). Stopped after the run.
- Data (outside git):
  - `$BOOK_DATA/nyc-taxi/zones/taxi_zones.zip` and `taxi_zones/` from `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip` (1.0 MB). Suggested addition to `code/common/get_data.sh`.
  - `$BOOK_DATA/citibike/raw/202606-citibike-tripdata_{1..6}.csv` from `https://s3.amazonaws.com/tripdata/202606-citibike-tripdata.zip` (1.05 GB, zip removed after unzip).
  - June 2026 yellow trips (existing).
- Scratch outputs: `data/work/ch76/` in the worktree (gitignored): `zones.parquet`, `points.parquet`, `pip.parquet`, `month_rows.parquet`, `geo_zones/`, `geo_stations/`, `warehouse/`.
- Bench: `pip_bench.py` ran under `flock ~/spark-masterclass/data/.bench.lock`, median of 3 after 1 warmup, a new DataFrame per run. Other writers' unlocked work may have shared the machine.
- Shared helpers: none added to `common/book.py`. Chapter helpers in `code/ch76/geo76.py` (shapefile reader, WKB writer, EPSG:2263 projection, error text).

## Open findings

1. **PySpark 4.3.0 knows only SRIDs 0, 3857 and 4326; the JVM registry has 10,713.** Any DataFrame with `GEOMETRY(srid)` for another registry SRID (2263, 32618, 4269, ...) fails in `df.schema` / `collect()` with `CANNOT_PARSE_DATATYPE [ST_INVALID_CRS_VALUE]` (Classic) or `ST_INVALID_SRID_VALUE` (Connect). Same on upstream/master (`python/pyspark/sql/geo_utils.py`, comment says "will be updated soon"). The docs page says GEOMETRY accepts any registry SRID. Repro: `spark.sql("SELECT st_geomfromwkb(X'0101000000000000000000F03F0000000000000040', 2263) g").schema`. Candidate JIRA (spark-bug skill first). Not called a bug in the book; the book gives a caution and the workaround.
2. **`GROUP BY` / `DISTINCT` on a geo column fails at run time with `scala.MatchError: ...BinaryView`** (already Part II open finding 3; still in 4.3.0, re-confirmed here).
3. **`df.show()` prints `org.apache.spark.unsafe.types.BinaryView@<hash>` for geo columns, and `to_json` fails with `_LEGACY_ERROR_TEMP_2026`.** Also on 4.2.0. Possibly a side effect of SPARK-57058 (BinaryView migration). Repro: `spark.sql("SELECT st_geogfromwkb(X'0101000000000000000000F03F0000000000000040') h").show()`.
4. **Iceberg PR #18749 (build b1eab64): `SELECT readable_metrics.geom.lower_bound FROM t.files` fails** with `IllegalStateException: Unknown type for binary field. Type name: org.apache.iceberg.geospatial.GeospatialBound` in `StructInternalRow.toByteArray`. Raw `lower_bounds[3]` works. Not Spark. Worth a review comment on the PR. Repro: `code/ch76/storage.py` §3.
5. Doc gap: `sql-ref-geospatial-types.md` "PROJ Version by Spark Release" table lists only 4.2.0 in the 4.3.0 docs.
6. Not verified: Apache Sedona support for Spark 4.3; the Hive SerDe refusal (SPARK-60519) was not run; GeoParquet metadata (the `geo` key-value metadata) is not written by Spark (not checked beyond the logical type).

## Clips and visuals

| File | What |
|---|---|
| `book/media/clips/ch76-inventory.mp4/.webp` | beat `inventory` of `ch76-chapter.mp4` (0-23.7 s), jump cut keeps 0-8.6 and 13.6-23.7 |
| `book/media/clips/ch76-pip.mp4/.webp` | beat `pip` (23.7-76.4 s), jump cut keeps 23.7-32.7 and 66.2-76.4 (the wait for Spark removed) |
| `book/media/clips/ch76-anim-bbox.mp4/.webp` | HyperFrames animation (create-animation kit, paper theme, landscape, 31.5 s), real zones and stations of lower Manhattan from `anim_data.py`; numbers from `pip.txt` and `pip_bench.txt`. Storyboard `code/ch76/anim/bbox-prefilter/storyboard.md`. Two storyboard beats are 0.2 s under the read-twice time (minor). Pacing not watched at 1x by a person. |
| `book/media/figures/ch76-types.svg` | hand-drawn type system figure (checked as PNG on light and dark) |
| `book/media/figures/ch76-map.svg` | two choropleth maps (bike starts by PIP, taxi pickups by zone ID), drawn by `render_map.py` from `trips_per_zone.csv` (164 KiB, checked on light and dark) |
| `book/media/figures/ch76-pairs.svg` | edge tests per method, log scale, from `pip.txt`, medians from `pip_bench.txt` |

Film: `film render code/film/ch76.tape --format chapter`, no bench lock. Encoding libx264 crf 28, yuv420p, faststart, no audio, poster 1.5 s before the end. Mermaid diagrams: 3 (WKB bytes, zones pipeline, grid PIP flow).

## Nav and roadmap rows for the integrator

```yaml
      - "10. Time series": chapters/ch10-time-series-geospatial.md
      - "76. Geospatial data in Spark 4.3": chapters/ch76-geospatial.md
```

Roadmap rows:

| # | Chapter | Covers |
|---|---|---|
| 10 | [Time series](chapters/ch10-time-series-geospatial.md) | `time_bucket`, gaps, sessions, percentiles. Geospatial moved to Chapter 76 |
| 76 | [Geospatial data in Spark 4.3](chapters/ch76-geospatial.md) | GEOMETRY and GEOGRAPHY, SRIDs, the five ST functions and what 4.3.0 does not have, WKB/WKT/GeoJSON by hand, point in polygon with a grid key and a bounding box on Citi Bike and the taxi zones, Parquet and Iceberg v3 geo types |

Coverage: WN05 geospatial and the geo half of MD21 move from Chapter 10 to Chapter 76 (Part II notes rows "Steps 8-15" now map to Chapter 76 Steps 2, 3, 10, 12, 13, 14). Suggested `get_data.sh` additions: the taxi zones zip and, optionally, the Citi Bike June 2026 zip.

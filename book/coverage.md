# Coverage

Spark Masterclass covers all that SparkTutor teaches, on Spark 4.3. This page maps each SparkTutor lesson to the part of the book that covers it. The table uses these abbreviations:

- LS is the Learning Spark course.
- MD is Messy Data and Python Data Sources.
- WN is What's New in Spark 4.x.
- SDP is Spark Declarative Pipelines.

| Part | Chapters | SparkTutor lessons |
|---|---|---|
| I | 1-4 | LS01 getting_started, LS11 spark_connect (client side), MD01 why_spark, MD02 setup_and_tour, WN01 spark4_at_a_glance |
| II | 5-10 | LS02 dataframes_schemas, LS05 spark_sql_deep_dive, LS07 join_strategies, MD03 schemas, MD04 cleaning_values, MD21 whats_new_4x (SQL parts), WN03 pipe_syntax_qualify, WN04 sql_scripting, WN05 geospatial, WN08 cdc_nearest_metrics (NEAREST BY) |
| III | 11-17 | LS03 data_sources, LS04 external_sources_hof, MD05 files_as_files, MD06 text_and_logs, MD07 csv_and_delimited, MD08 json_drift, MD09 complex_types, MD10 variant_basics, MD11 xml, MD12 avro_and_protobuf, MD13 documents_to_rows, MD14 images_and_audio, MD31 variant_shredding, MD47 blob_table_design, WN02 variant |
| IV | 18-24 | LS06 optimization_tuning, MD16 plans_partitions_caching, MD24 arrow_performance, MD30 python_udtfs, MD32 tuning_python, MD41 memory_and_blobs, MD45 parquet_internals; Project Feather (new in 4.3) |
| V | 25-29 | LS08 structured_streaming, MD17 streaming_files, MD33 streaming_messy_data, MD51 streaming_internals, WN07 transform_with_state; Real-time Mode (4.3) |
| VI | 30-36 | LS09 data_lakes, MD18 landing_in_delta, MD34 production_ingestion, SDP01-SDP09 (all nine), WN08 cdc_nearest_metrics (Auto CDC, metric views) |
| VII | 37-42 | LS10 mllib, MD19 text_analytics, MD20 inference_with_pandas_udfs, MD35 similarity_search, MD36 entity_resolution, MD37 inference_in_production, MD38 capstone_intermediate, MD48 lsh_and_ivf, MD49 connected_components, MD50 sketches |
| VIII | 43-49 | MD52 determinism_retries, MD53 shipping_dependencies, MD54 observability, LS11 spark_connect (server side); new: Connect deployment patterns, the Spark Kubernetes Operator, writing pipelines with agents (OSA Con 2026) |
| IX | 50-58 | MD15 first_python_data_source, MD22 custom_file_format, MD23 api_source, MD25 filter_pushdown, MD26 database_source, MD27 writers, MD28 streaming_sources, MD29 packaging_testing, MD42 java_dsv2_connector, MD43 session_extensions, MD44 connect_plugins, MD56 final_project, WN06 python_data_sources |
| X | 59-62 | MD39 catalyst_python_plans, MD40 python_worker_protocol, MD46 variant_binary_format, MD55 upstream_case_study |

In the repo, the notes of each part (`notes/partNN.md`) give each topic of each lesson, and the section that covers it. They also list the topics that the part does not cover, with the reason.

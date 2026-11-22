# An Airflow DAG hands off to Spark

Claim: each Airflow task starts its own Spark JVM in a child process of the worker; data moves between tasks through storage.
Data: `data.json`, from `code/ch63/output/dag_submit.txt` (task start and end times, log lines, run duration). Clip times are not to scale.

| beat | start | what moves | caption | why |
|---|---|---|---|---|
| task 1 | 0.5 | zone_revenue running, spark-submit JVM | Airflow runs the first task. Its worker starts spark-submit. | where the driver runs |
| log | 2.9 | log lines into the task log | The driver runs in that child JVM. Its log goes to the task log. | the hook streams the output |
| write | 5.4 | Parquet box, JVM gone, success 30 s | The job writes Parquet files, and the task ends with success. | the JVM ends with the task |
| task 2 | 7.4 | spark-sql JVM, read arrow | The next task starts a new JVM, and it reads those files. | no shared DataFrame |
| end | 10.4 | success 24 s, run 54.3 s | Data moves between the tasks through storage, not through Airflow. | the design rule |

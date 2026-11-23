# The operator follows a SparkApplication

Claim: the operator turns an applied SparkApplication into a driver pod, follows its states and writes them into the status.
Data: `data.json`, state times from `code/ch76/output/zones-app.txt` (one run). Clip times are chosen to read, not to scale.

| beat | start | what moves | caption | why |
|---|---|---|---|---|
| apply | 0.4 | YAML, operator, API | You apply a SparkApplication. The operator watches for it. | the declared resource |
| driver | 2.0 | driver pod, DriverRequested, DriverReady | The operator makes the driver pod and writes its state. | the operator's own work |
| executors | 5.0 | two executor pods, RunningHealthy | The driver asks the Kubernetes API for two executor pods. | who makes the executors |
| end | 8.4 | pods dim, Succeeded, TerminatedWithoutReleaseResources | The driver exits. The retain policy keeps the pods. | the end states |

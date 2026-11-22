# Animation: one collect() goes through the DAGScheduler and the TaskScheduler to two executors

**Claim.** The DAGScheduler cuts a job into stages at the shuffle; the TaskScheduler places each stage's tasks on executors. Chapter 71, Steps 3 and 4.

**Numbers.** Task index, executor and locality for each task, and the job time, from `code/ch71/output/scheduler_log.txt` via `../build_data.py`. Timing on screen is schematic.

**Backend:** hf. **Themes:** ide. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | title, driver and executor boxes | One job, from the driver to the executors (stays) | One job, from the driver to the executors. | topic at frame 0 |
| `job` | 0.6 | collect() drops into the DAGScheduler | One action becomes one job in the DAGScheduler | One action becomes one job. | where the work starts |
| `stages` | 3.4 | two stage boxes, the shuffle between them | The shuffle cuts the job into two stages | The shuffle cuts the job into two stages. | the stage boundary |
| `offers` | 6.4 | four task chips go to executors 0 and 1 | The TaskScheduler gives each task to a free core | The task scheduler gives each task to a free core. | placement |
| `stage2` | 10.0 | six chips for stage 2 | When stage 1 ends, the DAGScheduler submits stage 2 | When stage one ends, stage two starts. | dependency order |
| `result` | 13.6 | caption with the job time | Job done in 2.1 s: the results go back to the driver (stays) | The results go back to the driver. | ends on a measured number |

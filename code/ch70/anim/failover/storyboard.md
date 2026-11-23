# The standalone master dies and comes back

Claim: the master only hands out executors. After that, the driver and the executors work without it, and a master with FILESYSTEM recovery reads the application and the workers back from its folder.
Data: `code/ch70/output/ha.txt` (17.8 s SIGKILL, 28.6 s start, 31.2 s ALIVE, recovery 392 ms, app exits at 54.1 s with code 0, hold lines 1 to 30). Clip times are not to scale.

| beat | start | what moves | caption |
|---|---|---|---|
| run | 0.5 | tasks go from the driver to two executors | The driver sends tasks to its executors. The master only gave out the executors. |
| kill | 2.8 | master turns to "no master", tasks continue | The master gets SIGKILL at 17.8 s. The tasks continue. |
| start | 5.8 | new master, RECOVERING, folder reads | At 28.6 s a new master starts and reads the recovery folder. |
| rereg | 7.6 | workers and the app register again, ALIVE | The workers and the application register again. Recovery takes 392 ms. |
| end | 9.8 | app exits, code 0 | The application ends at 54.1 s with exit code 0. |

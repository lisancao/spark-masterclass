# Animation: the four installs differ in where the driver runs

**Claim.** pip and the tarball put a driver JVM next to your Python, Docker puts both in a container, Connect puts the driver behind gRPC. From Chapter 6.

**Numbers.** Python versions and the trip count from `code/ch06/output/check_env.txt` and the chapter's run of `hello.py` on each install.

**Backend:** hf (lanes and dots). **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | title, then the four rows | Where the driver runs (stays) | Four ways to install Spark. | topic at frame 0 |
| `local` | 1.2 | a query dot goes to the driver and back on rows 1 and 2 | pip and the tarball start a driver JVM next to your Python | pip and the tarball start a driver JVM next to your Python. | the default case |
| `docker` | 4.2 | dot inside the container frame | Docker puts Python and the driver in one container | Docker puts Python and the driver in one container. | the boundary moves |
| `connect` | 6.6 | dot crosses the machine edge over gRPC | Spark Connect sends the plan over gRPC to a server | Spark Connect sends the plan over gRPC to a server. | the driver leaves your machine |
| `same` | 9.0 | the counts stay | The same program gives the same count on each install (stays) | The same program gives the same count on each install. | the install changes where, not what |

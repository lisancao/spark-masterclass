"""Step 6: a reattachable execution. Run against `ch82/server.sh small-streams`, where the server
ends each response stream after 1 MiB, so a 20 MB result needs ReattachExecute calls."""
import os

from miniconnect import Session

s = Session(os.environ.get("SPARK_REMOTE", "sc://localhost:15106"))
df = s.sql("SELECT id, repeat('x', 100) AS pad FROM range(200000)")
print("plain ExecutePlan:", s.execute(df.relation).num_rows, "rows")
print("reattachable ExecutePlan:")
t = s.execute_reattachable(df.relation)
print(f"  {t.num_rows} rows, {t.nbytes / 2**20:.1f} MiB of Arrow data")
s.close()

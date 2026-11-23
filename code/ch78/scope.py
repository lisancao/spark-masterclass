"""Chapter 78, Step 5: what an agent's Connect client cannot reach, and a namespace for each agent.

Starts its own Connect server (port 10286) so that it does not share the server of the other
chapters. Run from code/:  uv run python ch78/scope.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ch30"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.book import ROOT  # noqa: E402
from connect_util import server, session, close, stop, error_class  # noqa: E402

PORT, UI = 10286, 4131
url = server(f"spark.sql.warehouse.dir={ROOT / 'data' / 'ch78' / 'warehouse'}", port=PORT, ui=UI)
agent = session(url, user_id="agent-7")


def probe(label, fn):
    try:
        print(f"  {label:44} {fn()}")
    except Exception as e:
        print(f"  {label:44} {error_class(e)}")


print("what the agent's Connect client cannot reach:")
probe("spark.sparkContext", lambda: agent.sparkContext)
probe("df.rdd", lambda: agent.range(3).rdd)
probe("spark._jvm", lambda: agent._jvm)
probe("spark.conf.set('spark.executor.memory')", lambda: agent.conf.set("spark.executor.memory", "64g"))

print("a namespace for the agent:")
agent.sql("CREATE NAMESPACE IF NOT EXISTS sandbox_agent_7")
agent.sql("CREATE NAMESPACE IF NOT EXISTS prod")
agent.sql("CREATE TABLE IF NOT EXISTS prod.daily_revenue AS SELECT 1 AS day, 100.0 AS revenue")
probe("write sandbox_agent_7.draft",
      lambda: agent.range(3).write.mode("overwrite").saveAsTable("sandbox_agent_7.draft") or "ok")
probe("overwrite prod.daily_revenue",
      lambda: agent.range(3).write.mode("overwrite").saveAsTable("prod.daily_revenue")
      or "ok (nothing refused it)")
print("  namespaces:", [r[0] for r in agent.sql("SHOW NAMESPACES").collect()])
agent.sql("DROP NAMESPACE IF EXISTS sandbox_agent_7 CASCADE")
agent.sql("DROP NAMESPACE IF EXISTS prod CASCADE")
close(agent)
stop(port=PORT, ui=UI)

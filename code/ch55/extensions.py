"""Chapter 55: which statements need the Iceberg SQL extensions on Spark 4.3.0.

Runs the same statements twice, in two processes: with spark.sql.extensions set, and without it.
The catalog is a JDBC catalog (SQLite), because a Hadoop catalog does not support views.
Run from code/:  uv run python ch55/extensions.py
"""
import os
import subprocess
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

STATEMENTS = [
    ("CREATE TABLE", "CREATE TABLE c.db.t (id BIGINT NOT NULL, fare DOUBLE) USING iceberg"),
    ("INSERT", "INSERT INTO c.db.t VALUES (1, 10.0), (2, 20.0)"),
    ("UPDATE", "UPDATE c.db.t SET fare = fare + 1 WHERE id = 1"),
    ("DELETE", "DELETE FROM c.db.t WHERE id = 2"),
    ("MERGE", "MERGE INTO c.db.t USING (SELECT 3L AS id, 30.0 AS fare) s ON t.id = s.id "
              "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"),
    ("VERSION AS OF (time travel)", "SELECT count(*) FROM c.db.t VERSION AS OF {first}"),
    ("SELECT ... FROM t@v<id> (new in 4.3)", "SELECT count(*) FROM c.db.t@v{first}"),
    ("spark.read.table('t@v<id>') (new in 4.3)", "PY:spark.read.table('c.db.t@v{first}').collect()"),
    ("CALL a procedure", "CALL c.system.rewrite_data_files(table => 'db.t')"),
    ("CREATE VIEW", "CREATE VIEW c.db.v AS SELECT id FROM c.db.t"),
    ("SELECT from the view", "SELECT count(*) FROM c.db.v"),
    ("COMMENT ON COLUMN ... IS NULL", "COMMENT ON COLUMN c.db.t.fare IS NULL"),
    ("ALTER TABLE ... CREATE BRANCH", "ALTER TABLE c.db.t CREATE BRANCH audit"),
    ("ALTER TABLE ... CREATE TAG", "ALTER TABLE c.db.t CREATE TAG v1"),
    ("ALTER TABLE ... ADD PARTITION FIELD", "ALTER TABLE c.db.t ADD PARTITION FIELD bucket(4, id)"),
    ("ALTER TABLE ... WRITE ORDERED BY", "ALTER TABLE c.db.t WRITE ORDERED BY id"),
    ("ALTER TABLE ... SET IDENTIFIER FIELDS", "ALTER TABLE c.db.t SET IDENTIFIER FIELDS id"),
]


def run(mode):
    """One process: run every statement, print 'name<TAB>result'."""
    os.environ["CH31_WORK"] = str(Path(os.environ.get("CH31_WORK", "")) / f"ext-{mode}")
    from ice import ice_session
    spark = ice_session(f"ch55-ext-{mode}", {"c": "jdbc"}, extensions=(mode == "on"))
    spark.sql("CREATE NAMESPACE IF NOT EXISTS c.db")
    first = None
    for name, sql in STATEMENTS:
        try:
            if sql.startswith("PY:"):
                eval(sql[3:].format(first=first))
            else:
                spark.sql(sql.format(first=first)).collect()
            result = "works"
        except Exception as e:
            msg = str(e).strip().splitlines()[0]
            result = msg.split("]")[0].lstrip("[") if msg.startswith("[") else msg[:60]
        if first is None and name == "INSERT":
            first = spark.sql("SELECT snapshot_id FROM c.db.t.snapshots").first()[0]
        print(f"{name}\t{result}", flush=True)
    if mode == "off":       # evidence that the view and the comment change went to Iceberg
        from ice import WORK
        views = sorted((WORK / "jdbc" / "db" / "v" / "metadata").glob("*.metadata.json"))
        print(f"#view\tc.db.v is an Iceberg view: {views[0].relative_to(WORK)}")
        spark.sql("COMMENT ON COLUMN c.db.t.fare IS 'fare in USD'")
        before = spark.sql("DESCRIBE c.db.t").collect()[1].comment
        spark.sql("COMMENT ON COLUMN c.db.t.fare IS NULL")
        after = spark.sql("DESCRIBE c.db.t").collect()[1].comment
        print(f"#comment\tcomment on fare: {before!r}, then after IS NULL: {after!r}")


if __name__ == "__main__" and len(sys.argv) > 1:
    run(sys.argv[1])
elif __name__ == "__main__":
    from ice import WORK
    import shutil
    results = {}
    for mode in ("on", "off"):
        shutil.rmtree(WORK / f"ext-{mode}", ignore_errors=True)
        out = subprocess.run([sys.executable, __file__, mode], capture_output=True, text=True,
                             env={**os.environ, "CH31_WORK": str(WORK)}).stdout
        results[mode] = dict(l.split("\t") for l in out.splitlines() if "\t" in l)
    print(f"Spark 4.3.0, runtime b1eab64, JDBC catalog\n")
    print(f"{'statement':<42} {'extensions on':<20} extensions off")
    for name, _ in STATEMENTS:
        print(f"{name:<42} {results['on'].get(name, '?'):<20} {results['off'].get(name, '?')}")
    print("\nwithout the extensions:")
    for k in ("#view", "#comment"):
        print("  " + results["off"][k])

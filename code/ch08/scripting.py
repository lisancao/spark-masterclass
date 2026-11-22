"""Chapter 8: SQL scripting. Variables, control flow, handlers, cursors and how a script runs.

Run from code/:  uv run python ch08/scripting.py
"""
import re
import tempfile
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from py4j.protocol import Py4JJavaError
from pyspark.errors import PySparkException
from common.book import session, ui_api, TRIPS, ZONES

spark = session("ch08-scripting", spark_sql_session_timeZone="UTC",
                spark_sql_warehouse_dir=tempfile.mkdtemp(prefix="ch08-"))
spark.read.parquet(str(TRIPS)).createOrReplaceTempView("trips")
spark.read.option("header", True).option("inferSchema", True).csv(str(ZONES)).createOrReplaceTempView("zones")


def err(e):
    if isinstance(e, PySparkException):
        first = next(l for l in str(e).splitlines() if l.strip())
        return f"{e.getCondition()}: " + first.split("] ", 1)[-1][:78]
    # A SqlScriptingException reaches Python as a plain Py4JJavaError in 4.3.0.
    m = re.search(r"(\w+(?:\.\w+)*Exception): (?:\{LINE:\d+\} )?\[([A-Z_.]+)\] ([^\n]*)", str(e))
    return f"{type(e).__name__} ({m.group(1).split('.')[-1]}) {m.group(2)}: {m.group(3)[:60]}"


def run(label, sql, **kw):
    try:
        df = spark.sql(sql, **kw)
        rows = df.collect()
        print(f"{label}: {rows if len(rows) != 1 else rows[0]}")
    except (PySparkException, Py4JJavaError) as e:
        print(f"{label}: {err(e)}")


# 1. Session variables live for the session. Script variables live inside BEGIN ... END.
print("== 1. session variables")
spark.sql("DECLARE OR REPLACE VARIABLE min_fare DOUBLE DEFAULT 50")
run("count with min_fare = 50", "SELECT count(*) AS n FROM trips WHERE fare_amount > min_fare")
spark.sql("SET VAR min_fare = 100")
run("count with min_fare = 100", "SELECT count(*) AS n FROM trips WHERE fare_amount > session.min_fare")
spark.sql("SET VAR min_fare = (SELECT percentile(fare_amount, 0.99) FROM trips)")
run("the variable after SET VAR = (query)", "SELECT min_fare")
run("a variable in a view", "CREATE TEMP VIEW big AS SELECT * FROM trips WHERE fare_amount > min_fare")

# 2. A script is one call to spark.sql. It returns the result of its last query.
print("\n== 2. a script, its control flow and its result")
FIB = """
BEGIN
  DECLARE a BIGINT DEFAULT 0;
  DECLARE b BIGINT DEFAULT 1;
  DECLARE i INT DEFAULT 0;
  WHILE i < :n DO
    SET (a, b) = (SELECT b, a + b);
    SET i = i + 1;
  END WHILE;
  SELECT :n AS n, a AS fib;
END"""
run("fib(10) with WHILE", FIB, args={"n": 10})
run("fib(90)", FIB, args={"n": 90})

run("IF / ELSEIF and CASE", """
BEGIN
  DECLARE n BIGINT;
  DECLARE label STRING;
  SET n = (SELECT count(*) FROM trips WHERE PULocationID = 132);
  IF n > 100000 THEN SET label = 'very busy';
  ELSEIF n > 10000 THEN SET label = 'busy';
  ELSE SET label = 'quiet';
  END IF;
  CASE WHEN label = 'very busy' THEN SELECT 'JFK' AS zone, n, label;
       ELSE SELECT 'other' AS zone, n, label;
  END CASE;
END""")

run("REPEAT, LOOP, LEAVE and ITERATE", """
BEGIN
  DECLARE i INT DEFAULT 0;
  DECLARE odd_sum INT DEFAULT 0;
  DECLARE r INT DEFAULT 0;
  REPEAT SET r = r + 1; UNTIL r >= 3 END REPEAT;
  lbl: LOOP
    SET i = i + 1;
    IF i > 9 THEN LEAVE lbl; END IF;
    IF i % 2 = 0 THEN ITERATE lbl; END IF;
    SET odd_sum = odd_sum + i;
  END LOOP lbl;
  SELECT r AS repeats, odd_sum;
END""")

# 3. FOR iterates over the rows of a query. Each row is a struct with the query's columns.
print("\n== 3. FOR over a query: one summary row per borough")
spark.sql("DROP TABLE IF EXISTS borough_summary")
spark.sql("CREATE TABLE borough_summary (borough STRING, zones INT, trips BIGINT) USING parquet")
run("FOR loop", """
BEGIN
  FOR b AS SELECT Borough AS name, count(*) AS zones FROM zones
           WHERE Borough NOT IN ('N/A', 'Unknown') GROUP BY Borough DO
    INSERT INTO borough_summary
      SELECT b.name, b.zones, count(*) FROM trips t JOIN zones z ON t.PULocationID = z.LocationID
      WHERE z.Borough = b.name;
  END FOR;
  SELECT * FROM borough_summary ORDER BY trips DESC;
END""")

# 4. How a script runs: the driver runs one statement at a time, and each query is its own job.
print("\n== 4. how a script runs")
before = {j["jobId"] for j in ui_api("jobs")}
sql_before = {q["id"] for q in ui_api("sql?details=false&length=10000")}
spark.sql("""
BEGIN
  DECLARE i INT DEFAULT 0;
  DECLARE total BIGINT DEFAULT 0;
  WHILE i < 5 DO
    SET total = total + (SELECT count(*) FROM trips WHERE PULocationID = 132 + i);
    SET i = i + 1;
  END WHILE;
  SELECT total;
END""").collect()
time.sleep(3)   # the UI's listener runs a little behind the driver
new_jobs = [j for j in ui_api("jobs") if j["jobId"] not in before]
new_sql = [q for q in ui_api("sql?details=false&length=10000") if q["id"] not in sql_before]
print(f"one spark.sql() call with a 5-pass loop -> {len(new_sql)} SQL executions, {len(new_jobs)} jobs")
kinds = {}
for q in ui_api("sql?details=true&planDescription=true&length=10000"):
    if q["id"] in {x["id"] for x in new_sql}:
        reads = "Scan parquet" in q["planDescription"]
        kinds[reads] = kinds.get(reads, 0) + 1
print(f"    executions that scan the trips: {kinds.get(True, 0)}, executions with no scan: {kinds.get(False, 0)}")
b2 = {j["jobId"] for j in ui_api("jobs")}
total = spark.sql("SELECT count(*) FROM trips WHERE PULocationID BETWEEN 132 AND 136").first()[0]
time.sleep(3)
print("the same total as one query:", total, f"({len([j for j in ui_api('jobs') if j['jobId'] not in b2])} jobs)")

# 5. Handlers. EXIT runs the handler and leaves the block.
print("\n== 5. condition handlers")
run("EXIT handler for DIVIDE_BY_ZERO", """
BEGIN
  DECLARE result STRING DEFAULT 'not set';
  BEGIN
    DECLARE EXIT HANDLER FOR DIVIDE_BY_ZERO SET result = 'caught DIVIDE_BY_ZERO';
    SET result = CAST(1 / 0 AS STRING);
    SET result = 'after the error';
  END;
  SELECT result;
END""")
run("the most specific handler wins", """
BEGIN
  DECLARE result STRING;
  BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION SET result = 'SQLEXCEPTION';
    DECLARE EXIT HANDLER FOR SQLSTATE '22012' SET result = 'SQLSTATE 22012';
    DECLARE EXIT HANDLER FOR DIVIDE_BY_ZERO SET result = 'DIVIDE_BY_ZERO';
    SELECT 1 / 0;
  END;
  SELECT result;
END""")
run("a named condition", """
BEGIN
  DECLARE result STRING;
  DECLARE bad_cast CONDITION FOR SQLSTATE '22018';
  BEGIN
    DECLARE EXIT HANDLER FOR bad_cast SET result = 'caught bad_cast';
    SELECT CAST('n/a' AS INT);
  END;
  SELECT result;
END""")
run("no handler: the script stops", """
BEGIN
  INSERT INTO borough_summary VALUES ('test', 0, 0);
  SELECT CAST('n/a' AS INT);
  INSERT INTO borough_summary VALUES ('never', 0, 0);
END""")
run("rows after the failed script", "SELECT borough FROM borough_summary WHERE borough IN ('test', 'never')")
run("a CONTINUE handler (default config)", """
BEGIN
  DECLARE n INT DEFAULT 0;
  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET n = n + 1;
  SELECT 1 / 0;
  SELECT n;
END""")
spark.conf.set("spark.sql.scripting.continueHandlerEnabled", "true")
run("a CONTINUE handler (flag on)", """
BEGIN
  DECLARE errors INT DEFAULT 0;
  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET errors = errors + 1;
  SELECT 1 / 0;
  SELECT CAST('n/a' AS INT);
  SELECT errors, 'reached the end' AS status;
END""")

# 6. Cursors: behind an internal flag in 4.3.0.
print("\n== 6. cursors")
CURSOR = """
BEGIN
  DECLARE total DOUBLE DEFAULT 0;
  DECLARE f DOUBLE;
  DECLARE done BOOLEAN DEFAULT false;
  DECLARE c CURSOR FOR SELECT fare_amount FROM trips WHERE PULocationID = 1 AND fare_amount > 0 LIMIT 5;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = true;
  OPEN c;
  REPEAT
    FETCH c INTO f;
    IF NOT done THEN SET total = total + f; END IF;
  UNTIL done END REPEAT;
  CLOSE c;
  SELECT round(total, 2) AS total_of_5_fares;
END"""
spark.conf.unset("spark.sql.scripting.continueHandlerEnabled")
run("cursor (default config)", CURSOR)
spark.conf.set("spark.sql.scripting.cursorEnabled", "true")
spark.conf.set("spark.sql.scripting.continueHandlerEnabled", "true")
run("cursor (both flags on)", CURSOR)
run("check with one query", "SELECT round(sum(fare_amount), 2) FROM (SELECT fare_amount FROM trips "
                            "WHERE PULocationID = 1 AND fare_amount > 0 LIMIT 5)")
for k in ("spark.sql.scripting.cursorEnabled", "spark.sql.scripting.continueHandlerEnabled"):
    spark.conf.unset(k)

# 7. EXECUTE IMMEDIATE: SQL text built at run time, with parameters and INTO.
print("\n== 7. EXECUTE IMMEDIATE")
spark.sql("DECLARE OR REPLACE VARIABLE n BIGINT")
spark.sql("DECLARE OR REPLACE VARIABLE tbl STRING DEFAULT 'zones'")
spark.sql("EXECUTE IMMEDIATE 'SELECT count(*) FROM ' || tbl INTO n")
run("count of a table named at run time", "SELECT tbl, n")
run("USING with named markers", "EXECUTE IMMEDIATE "
    "'SELECT count(*) AS n FROM trips WHERE PULocationID = :zone AND fare_amount > :fare' "
    "USING 132 AS zone, 70 AS fare")
run("USING with positional markers", "EXECUTE IMMEDIATE "
    "'SELECT count(*) AS n FROM trips WHERE PULocationID = ? AND fare_amount > ?' USING 132, 70")
run("in a script, one count per table", """
BEGIN
  DECLARE out STRING DEFAULT '';
  DECLARE c BIGINT;
  FOR t AS SELECT * FROM VALUES ('zones'), ('borough_summary') AS v(name) DO
    EXECUTE IMMEDIATE 'SELECT count(*) FROM ' || t.name INTO c;
    SET out = out || t.name || '=' || c || ' ';
  END FOR;
  SELECT trim(out) AS counts;
END""")
run("INTO with two rows", "BEGIN DECLARE x INT; EXECUTE IMMEDIATE 'SELECT id FROM range(2)' INTO x; END")

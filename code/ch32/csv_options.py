"""Chapter 32, part 2: what the CSV reader does with real files. Headers, schemas, inference,
separators, quotes, line breaks, NULLs, whitespace, dates, decimal commas and encodings.

Run from code/:  uv run python ch32/csv_options.py
"""
import shutil
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, show, MESSY, WORK

spark = session("ch32-csv-options", spark_sql_session_timeZone="UTC")
D = MESSY / "delimited"
tmp = WORK / "ch32" / "csv"
shutil.rmtree(tmp, ignore_errors=True)
tmp.mkdir(parents=True)


def section(title):
    print(f"\n== {title} ==")


def csv_file(name, text, encoding="utf-8"):
    path = tmp / name
    with open(path, "w", encoding=encoding, newline="") as f:
        f.write(text)
    return str(path)


def err(e, n=110):
    first = str(e).strip().splitlines()[0]
    return first[:n]


def jobs_started(fn):
    """How many Spark jobs `fn` starts, counted with a job group and the status tracker."""
    sc = spark.sparkContext
    sc.setJobGroup("probe", "probe")
    before = set(sc.statusTracker().getJobIdsForGroup("probe"))
    fn()
    n = len(set(sc.statusTracker().getJobIdsForGroup("probe")) - before)
    sc.setJobGroup("other", "other")
    return n


section("headers")
p = csv_file("headers.csv", "id,name,id,,  city  \n1,Ada,7,x,Toronto\n")
print("header=false:", spark.read.csv(p).columns)
print("header=true: ", spark.read.option("header", True).csv(p).columns)
print("ID,id:       ", spark.read.option("header", True).csv(csv_file("case.csv", "ID,id\n1,2\n")).columns)
odd = spark.read.option("header", True).csv(csv_file("odd.csv", "first name,amount (CAD),e.mail\nAda,1,a@x.ca\n"))
try:
    odd.select("e.mail").collect()
except Exception as e:
    print('select("e.mail") ->', err(e, 80))
print("select(\"`e.mail`\") ->", odd.select("`e.mail`").first()[0])

section("enforceSchema: the schema wins by position, silently")
swapped = csv_file("swapped.csv", "name,id\nAda,1\nBao,2\n")
show(spark.read.option("header", True).schema("id INT, name STRING").csv(swapped))
try:
    (spark.read.option("header", True).option("enforceSchema", False)
     .schema("id INT, name STRING").csv(swapped).collect())
except Exception as e:
    s = str(e)
    i = s.find("CSV header does not conform")
    print("enforceSchema=false ->", s[i:].splitlines()[0] if i >= 0 else err(e))
    for line in s[i:].splitlines()[1:4]:
        print("   ", line.strip())

section("inferSchema: what it guesses")
inv = csv_file("infer.csv",
               "sku,zip,qty,price,day,ts,flag,mixed,padded\n"
               "00123,02139,1,1.50,2026-01-05,2026-01-05 10:00:00,true,7, 7 \n"
               "00456,10001,2,2.25,2026-01-06,2026-01-06 11:30:00,false,x, 8 \n")
df = spark.read.option("header", True).option("inferSchema", True).csv(inv)
print(df.schema.simpleString())
show(df)

section("inferSchema: what it costs, in jobs, before any action")
prod = str(D / "products.csv")
print("schema given        :", jobs_started(lambda: spark.read.schema("sku STRING").csv(prod)), "jobs")
print("header only         :", jobs_started(lambda: spark.read.option("header", True).csv(prod)), "jobs")
print("header + inferSchema:", jobs_started(
    lambda: spark.read.option("header", True).option("inferSchema", True).csv(prod)), "jobs")
nums = csv_file("sampled.csv", "n\n" + "".join(f"{i}\n" for i in range(1000)) + "hello\n")
for ratio in (1.0, 0.1):
    d = (spark.read.option("header", True).option("inferSchema", True)
         .option("samplingRatio", ratio).csv(nums))
    print(f"samplingRatio={ratio}: n is {d.schema['n'].dataType.simpleString():6}"
          f" and the row 'hello' reads as {d.collect()[-1][0]!r}")

section("separators")
semi = csv_file("semi.csv", "a;b\n1,5;2\n")
print("default sep:", spark.read.option("header", True).csv(semi).collect())
print("sep=';'    :", spark.read.option("header", True).option("sep", ";").csv(semi).collect())
print("sep='||'   :", spark.read.option("sep", "||").csv(csv_file("multi.csv", "1|x||2\n")).collect())
try:
    spark.read.option("sep", "").csv(semi).collect()
except Exception as e:
    print("sep=''     :", err(e, 70))

section("quotes and escapes: RFC 4180 or backslash")
q = csv_file("quotes.csv", 'id,note\n1,"hello, world"\n2,"she said ""hi"""\n3,"back\\"slash"\n')
show(spark.read.option("header", True).csv(q))
show(spark.read.option("header", True).option("escape", '"').csv(q))

section("stray quotes: unescapedQuoteHandling")
sq = csv_file("stray.csv", 'id,note,n\n1,ok,5\n2,"x"y,z",8\n')
for mode in ("STOP_AT_DELIMITER", "STOP_AT_CLOSING_QUOTE", "BACK_TO_DELIMITER", "SKIP_VALUE", "RAISE_ERROR"):
    try:
        row = (spark.read.option("header", True).option("unescapedQuoteHandling", mode)
               .csv(sq).collect()[1])
        print(f"  {mode:22} -> note={row.note!r}, n={row.n!r}")
    except Exception as e:
        s = str(e)
        i = s.find("Unescaped quote")
        print(f"  {mode:22} -> fails: {s[i:i+75] if i >= 0 else err(e, 75)}")

section("line breaks inside quoted fields: multiLine")
naive = spark.read.option("header", True).csv(prod)
print("line mode rows:", naive.count())
show(naive.select("sku", "name", "description").limit(3))
good = spark.read.option("header", True).option("multiLine", True).option("escape", '"').csv(prod)
print("multiLine + escape rows:", good.count())
show(good.select("sku", "name", "description").limit(3))
un = csv_file("unterminated.csv", 'id,note\n1,"never closed\n2,b\n3,c\n')
print("unterminated quote, line mode:", spark.read.option("header", True).csv(un).collect())
print("unterminated quote, multiLine:", spark.read.option("header", True).option("multiLine", True).csv(un).collect())

section("NULL, empty and NaN")
nv = csv_file("nulls.csv", 'id,s,x\n1,,1.5\n2,"",NaN\n3,NA,NA\n4,  ,-Inf\n5,null,Inf\n6,N/A,inf\n')
sch = "id INT, s STRING, x DOUBLE"
a = spark.read.option("header", True).schema(sch).csv(nv).collect()
b = spark.read.option("header", True).schema(sch).option("nullValue", "NA").csv(nv).collect()
print(f"{'raw line':14} {'default s':>10} {'x':>10}   {'nullValue=NA s':>14} {'x':>10}")
raw_lines = open(nv).read().splitlines()[1:]
for line, r1, r2 in zip(raw_lines, a, b):
    print(f"{line:14} {r1.s!r:>10} {r1.x!r:>10}   {r2.s!r:>14} {r2.x!r:>10}")

section("whitespace: each type parser has its own rule")
ws = csv_file("ws.csv", " 7 , 2.5 , 2026-01-01 \n")
print("default:", spark.read.schema("a INT, b DOUBLE, c DATE").csv(ws).collect())
print("trimmed:", spark.read.schema("a INT, b DOUBLE, c DATE").option("ignoreLeadingWhiteSpace", True)
      .option("ignoreTrailingWhiteSpace", True).csv(ws).collect())
print("SQL CAST(' 7 ' AS INT):", spark.sql("SELECT CAST(' 7 ' AS INT)").first()[0])

section("dates and the European export")
eu = str(D / "orders_eu.csv")
plain = spark.read.option("header", True).option("sep", ";").option("comment", "#")
s6 = "order_id INT, sku STRING, customer STRING, amount STRING, order_date DATE, note STRING"
show(plain.schema(s6).csv(eu).select("order_id", "amount", "order_date").limit(3))
show(plain.schema(s6).option("dateFormat", "dd.MM.yyyy").csv(eu).select("order_id", "amount", "order_date").limit(3))
fr = csv_file("fr.csv", "6 janv. 2026\n")
for loc in ("en-US", "fr-FR"):
    print(f"'6 janv. 2026', dateFormat='d MMM yyyy', locale={loc}:",
          spark.read.schema("d DATE").option("dateFormat", "d MMM yyyy").option("locale", loc).csv(fr).first()[0])

tz = csv_file("tz.csv", "2026-01-05 10:00:00\n")
print("'2026-01-05 10:00:00' as TIMESTAMP, timeZone=America/Toronto:",
      spark.read.schema("t TIMESTAMP").option("timeZone", "America/Toronto").csv(tz).selectExpr("string(t)").first()[0], "(session time zone UTC)")
print("'2026-01-05 10:00:00' as TIMESTAMP_NTZ, same option:          ",
      spark.read.schema("t TIMESTAMP_NTZ").option("timeZone", "America/Toronto").csv(tz).selectExpr("string(t)").first()[0])

section("decimal commas")
for t in ("DECIMAL(10,2)", "DOUBLE"):
    for loc in ("en-US", "de-DE"):
        r = (plain.schema(f"order_id INT, sku STRING, customer STRING, amount {t}")
             .option("locale", loc).csv(eu).limit(4).collect())
        print(f"{t:13} {loc}:", [str(x.amount) if x.amount is not None else None for x in r])
print("file says:     ", [l.split(";")[3] for l in open(eu, encoding="utf-8-sig").read().splitlines()[3:7]])

section("encodings")
lat = str(D / "legacy_latin1.tsv")
show(spark.read.option("header", True).option("sep", "\t").csv(lat).select("name1", "city").limit(2))
show(spark.read.option("header", True).option("sep", "\t").option("encoding", "ISO-8859-1")
     .csv(lat).select("name1", "city").limit(2))
u16 = csv_file("utf16.csv", "id,name\n1,Zoë\n2,Nguyễn\n", encoding="utf-16")
print("UTF-16, line mode:", spark.read.option("header", True).option("encoding", "UTF-16").csv(u16).collect())
print("UTF-16, multiLine:", spark.read.option("header", True).option("encoding", "UTF-16")
      .option("multiLine", True).csv(u16).collect())
cp = csv_file("cp1252.csv", "id,name\n1,“quoted” €5\n", encoding="cp1252")
try:
    spark.read.option("header", True).option("encoding", "windows-1252").csv(cp).collect()
except Exception as e:
    print("windows-1252:", err(e, 220))
print("as ISO-8859-1:", repr(spark.read.option("header", True).option("encoding", "ISO-8859-1").csv(cp).first().name))

section("byte order mark and comment lines")
bom = csv_file("bom.csv", "id,name\n1,Ada\n", encoding="utf-8-sig")
print("first bytes:", open(bom, "rb").read()[:6], " columns:", spark.read.option("header", True).csv(bom).columns,
      " len(columns[0]):", len(spark.read.option("header", True).csv(bom).columns[0]))
print("wholetext keeps it:", repr(spark.read.text(bom, wholetext=True).first()[0][:3]))
print("no comment option:", spark.read.option("header", True).option("sep", ";").csv(eu).columns)
try:
    spark.read.option("comment", "//").csv(eu).collect()
except Exception as e:
    print("comment='//':", err(e, 80))

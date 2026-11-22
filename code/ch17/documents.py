"""Chapter 17, part 1: documents to rows. A mailbox export arrives as a zip (the 4.3 archive reader),
e-mail parsed in a Python UDF, PDF pages from a Python UDTF with lateralJoin, Markdown chunks with
built-in functions, and one document table with provenance and hashes.

Run from code/:  uv run python ch17/documents.py
"""
import re
import shutil
import sys
import zipfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F
from common.book import session, show, plan, MESSY, WORK

spark = session("ch17-documents", spark_sql_session_timeZone="UTC")
TMP = WORK / "ch17"
shutil.rmtree(TMP, ignore_errors=True)
TMP.mkdir(parents=True)


def section(title):
    print(f"\n== {title} ==")


def condition(e):
    m = re.search(r"\[([A-Z_0-9.]+)\]", str(e))
    return m.group(1) if m else f"{type(e).__name__}: {str(e).splitlines()[0][:60]}"


def attempt(label, fn):
    try:
        print(f"  {label:44} -> {fn()}")
    except Exception as e:
        print(f"  {label:44} -> {condition(e)}")


def nodes(df):
    """The operator names of the physical plan, without ids."""
    return [re.sub(r"[\s+\-*()0-9:|]+", " ", l).split()[0] for l in plan(df, "simple").splitlines()[1:] if l.strip()]


section("the export: a zip of e-mails and invoices")
export = TMP / "support_export.zip"
with zipfile.ZipFile(export, "w", zipfile.ZIP_DEFLATED) as z:
    for f in sorted((MESSY / "mail").glob("*.eml")) + sorted((MESSY / "pdf").glob("*.pdf")):
        z.writestr(zipfile.ZipInfo(f"export/{f.parent.name}/{f.name}", date_time=(2026, 10, 4, 0, 0, 0)),
                   f.read_bytes(), zipfile.ZIP_DEFLATED)
    z.writestr(zipfile.ZipInfo("export/mail/notes.txt", date_time=(2026, 10, 4, 0, 0, 0)), b"not an e-mail\n")
spark.conf.set("spark.sql.files.archive.reader.enabled", "true")
entries = (spark.read.format("binaryFile").option("wholeFile", "false").load(str(export))
           .select(F.regexp_replace("path", r"^.*/", "").alias("name"),
                   F.regexp_extract("path", r"!/(.*)$", 1).alias("entry"), "length", "content"))
show(entries.select("entry", "length", F.hex(F.substring("content", 1, 5)).alias("first 5 bytes")).orderBy("entry"))
print("Python type of content:", type(entries.first().content).__name__)

section("e-mail: a plain Python parser, then a UDF")
EMAIL_TYPE = "struct<sender:string, subject:string, sent:timestamp, body:string, error:string>"


def parse_eml(content):
    """(sender, subject, sent, body, error) from the bytes of one e-mail. Never raises."""
    from email import policy
    from email.parser import BytesParser
    try:
        msg = BytesParser(policy=policy.default).parsebytes(content)
        if msg["From"] is None:
            return (None, None, None, None, "no From header")
        date = msg["Date"]
        body = msg.get_body(preferencelist=("plain",))
        return (str(msg["From"]), str(msg["Subject"]), date.datetime if date else None,
                body.get_content().strip() if body else None, None)
    except Exception as e:                       # one odd file must not fail the job
        return (None, None, None, None, f"{type(e).__name__}: {e}")


parse = F.udf(parse_eml, EMAIL_TYPE)
mail = entries.where(F.col("entry").startswith("export/mail/"))
emails = (mail.select("entry", parse("content").alias("m")).select("entry", "m.*")
          .withColumn("order_id", F.regexp_extract("body", r"order (\d+)", 1).try_cast("int")))
show(emails.select("entry", "sender", "subject", F.date_format("sent", "yyyy-MM-dd HH:mm").alias("sent_utc"),
                   "order_id", "error").orderBy("entry"))
print("collect() gives sent as a naive datetime in the driver's zone:",
      emails.where("entry LIKE '%ticket_2%'").first().sent)
print("plan:", " <- ".join(nodes(emails)[:3]))
spark.conf.set("spark.sql.execution.pythonUDF.arrow.enabled", "false")
print("pythonUDF.arrow.enabled=false, a new UDF:", " <- ".join(nodes(
    mail.select(F.udf(parse_eml, EMAIL_TYPE)("content")))[:3]))
spark.conf.unset("spark.sql.execution.pythonUDF.arrow.enabled")

section("PDF: one row per page, from a Python UDTF")
pdf_raw = entries.where(F.col("entry").endswith(".pdf"))
sample = pdf_raw.first().content
print("PDF header:", sample[:8], " page objects:", sample.count(b"/Type /Page") - sample.count(b"/Type /Pages"))
print("one content stream:", re.search(rb"stream\r?\n(.*?)endstream", sample, re.S).group(1)[:60])


class PdfPages:
    """One row (page, text) for each page of the sample PDFs: each page draws BT ... (text) Tj ET."""
    def eval(self, content):
        import re
        if content is None:
            return
        for n, raw in enumerate(re.findall(rb"BT .*?\((.*?)\) Tj ET", content, re.S), start=1):
            yield n, raw.decode("latin-1")


pages_udtf = F.udtf(PdfPages, returnType="page int, text string")
pages = pdf_raw.select("entry", "content").lateralJoin(pages_udtf(F.col("content").outer())).drop("content")
show(pages.orderBy("entry", "page"))
print("plan:", " <- ".join(nodes(pages)[:4]))
attempt("lateralJoin without .outer()", lambda: pdf_raw.lateralJoin(pages_udtf(F.col("content"))).count())
spark.udtf.register("pdf_pages", pages_udtf)
pdf_raw.createOrReplaceTempView("pdf_raw")
print("SQL LATERAL pdf_pages(content):", spark.sql("SELECT entry, p.* FROM pdf_raw, LATERAL pdf_pages(content) p").count(), "rows")

section("a UDTF that gives no rows: the inner and the left lateral join")
with_empty = pdf_raw.select("entry", "content").unionByName(
    spark.createDataFrame([("export/pdf/blank.pdf", b"%PDF-1.4\n%%EOF\n")], "entry STRING, content BINARY"))
print("inner lateralJoin:                ", with_empty.lateralJoin(pages_udtf(F.col("content").outer())).select("entry").distinct().count(), "of 3 files")
print("lateralJoin(how='left'):          ", with_empty.lateralJoin(pages_udtf(F.col("content").outer()), how="left").select("entry").distinct().count(), "of 3 files")
print("lateralJoin(on=lit(True), 'left'):", with_empty.lateralJoin(pages_udtf(F.col("content").outer()), on=F.lit(True), how="left").select("entry").distinct().count(), "of 3 files")
with_empty.createOrReplaceTempView("with_empty")
print("SQL LEFT JOIN LATERAL ... ON true:", spark.sql(
    "SELECT DISTINCT entry FROM with_empty LEFT JOIN LATERAL pdf_pages(content) p ON true").count(), "of 3 files")

section("Markdown: chunks with built-in functions only")
ws = F.lit(" \t\r\n")
docs = spark.read.text(str(MESSY / "docs" / "*.md"), wholetext=True)
paragraphs = F.filter(F.split("value", r"\n\s*\n"), lambda p: (F.trim(p, ws) != "") & ~F.trim(p, ws).startswith("#"))
chunks = (docs.select(F.col("_metadata.file_name").alias("file"),
                      F.regexp_extract("value", r"(?m)^# (.+)$", 1).alias("title"),
                      F.posexplode(paragraphs).alias("pos", "text"))
          .select("file", "title", (F.col("pos") + 1).alias("chunk"), F.trim("text", ws).alias("text")))
show(chunks.select("file", "title", "chunk", F.substring("text", 1, 48).alias("text")).orderBy("file", "chunk"), 6)
print("plan has a Python node:", any("Python" in n for n in nodes(chunks)))

section("one document table, with provenance and a content hash")
documents = (emails.where("error IS NULL").select(F.col("entry").alias("doc"), F.lit(1).alias("part"),
                                                  F.lit("email").alias("kind"), F.col("subject").alias("title"),
                                                  F.col("body").alias("text"))
             .unionByName(pages.select(F.col("entry").alias("doc"), F.col("page").alias("part"),
                                       F.lit("pdf").alias("kind"), F.lit(None).cast("string").alias("title"), "text"))
             .unionByName(chunks.select(F.col("file").alias("doc"), F.col("chunk").alias("part"),
                                        F.lit("markdown").alias("kind"), "title", "text"))
             .withColumn("sha", F.sha2(F.lower(F.trim("text")), 256)))
show(documents.groupBy("kind").agg(F.count("*").alias("rows"), F.countDistinct("sha").alias("distinct texts")).orderBy("kind"))
print("all rows:", documents.count(), " distinct texts:", documents.select("sha").distinct().count())

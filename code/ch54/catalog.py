"""Chapter 54: turn a folder of mixed files into one row per document part.

The same datasets as the SparkTutor capstone's catalog pipeline, as one batch script:
an inventory from binaryFile without the bytes, PDF pages, e-mails and text files from a UDF
that returns an array of parts, images from the image source, WAV durations from a UDF.
Run from code/:  uv run python ch54/catalog.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F, types as T
from common.book import session, plan, ROOT

MESSY = ROOT / "code" / "data" / "messy"
INPUT = str(MESSY) + "/{audio,docs,events,images,logs,mail,pdf,xml}"
KINDS = {".png": "image", ".wav": "audio", ".pdf": "pdf", ".eml": "email",
         ".md": "text", ".txt": "text", ".log": "log", ".json": "json", ".xml": "xml"}
def kind():
    """The kind of a file, from its extension (a Column; built inside a session)."""
    ext = F.lower(F.regexp_extract(F.col("path"), r"(\.[A-Za-z0-9]+)$", 1))
    return F.coalesce(F.create_map(*[F.lit(x) for kv in KINDS.items() for x in kv])[ext], F.lit("other"))
# Type objects, not DDL strings: a DDL string needs a running session to parse at decoration time.
PART_TYPE = T.ArrayType(T.StructType([T.StructField("part", T.IntegerType()),
                                      T.StructField("title", T.StringType()),
                                      T.StructField("text", T.StringType())]))


@F.udf(T.DoubleType())
def wav_seconds(content):
    import io, wave
    with wave.open(io.BytesIO(content)) as w:
        return w.getnframes() / w.getframerate()


@F.udf(PART_TYPE)
def split_parts(kind, content):
    """One entry per part: each PDF page, one e-mail, or a whole text file."""
    import re
    from email import policy
    from email.parser import BytesParser
    if kind == "pdf":
        pages = re.findall(rb"BT .*?\((.*?)\) Tj ET", content, re.S)   # tiny uncompressed PDFs only
        return [(n, None, t.decode("latin-1")) for n, t in enumerate(pages, 1)]
    if kind == "email":
        msg = BytesParser(policy=policy.default).parsebytes(content)
        body = msg.get_body(("plain",))
        return [(0, str(msg["Subject"]), body.get_content().strip() if body else None)]
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        text = content.decode("latin-1")
    first = text.splitlines()[0] if text else ""
    return [(0, first.lstrip("# ").strip() or None, text)]


def build_catalog(spark):
    inventory = (spark.read.format("binaryFile").option("recursiveFileLookup", "true").load(INPUT)
                 .select(F.col("_metadata.file_path").alias("path"),
                         F.col("_metadata.file_name").alias("file_name"),
                         F.col("length").alias("size"))
                 .withColumn("kind", kind()))
    images = (spark.read.format("image").option("dropInvalid", "true").option("recursiveFileLookup", "true")
              .load(str(MESSY / "images"))
              .select(F.col("_metadata.file_path").alias("path"),
                      F.col("image.width").alias("width"), F.col("image.height").alias("height")))
    audio = (spark.read.format("binaryFile").option("pathGlobFilter", "*.wav").load(str(MESSY / "audio"))
             .select(F.col("_metadata.file_path").alias("path"), wav_seconds("content").alias("seconds")))
    parts = (spark.read.format("binaryFile").option("recursiveFileLookup", "true").load(INPUT)
             .select(F.col("_metadata.file_path").alias("path"), "content")
             .withColumn("kind", kind())
             .filter(F.col("kind").isin("pdf", "email", "text", "log", "json", "xml"))
             .select("path", F.explode(split_parts("kind", "content")).alias("p"))
             .select("path", "p.part", "p.title", "p.text"))
    return (inventory.join(parts, "path", "left").join(images, "path", "left").join(audio, "path", "left")
            .select("path", "file_name", "kind", F.coalesce("part", F.lit(0)).alias("part"),
                    "size", "width", "height", "seconds", "title", "text"))


if __name__ == "__main__":
    spark = session("ch54-catalog", spark_ui_showConsoleProgress="false")
    catalog = build_catalog(spark).cache()
    print(f"{catalog.count()} parts from {catalog.select('path').distinct().count()} files\n")
    (catalog.groupBy("kind").agg(F.count_distinct("path").alias("files"), F.count("*").alias("parts"))
     .orderBy("kind").show())
    (catalog.where("kind IN ('pdf', 'email', 'image', 'audio')")
     .select("file_name", "kind", "part", "width", "seconds",
             F.substring(F.coalesce("title", F.lit("")), 1, 28).alias("title"),
             F.substring(F.regexp_replace("text", r"\s+", " "), 1, 30).alias("text"))
     .orderBy("kind", "file_name", "part").show(truncate=False))
    print(catalog.where("file_name = 'notes_latin1.txt'").select("text").first()[0].strip())
    print()
    # The inventory reads no bytes: binaryFile without the content column.
    inv = spark.read.format("binaryFile").option("recursiveFileLookup", "true").load(INPUT).select("path", "length")
    print([l.strip()[:110] for l in plan(inv, "simple").splitlines() if "FileScan" in l][0])

    # The catalog is the input of a search index: one row per part, any kind that has text.
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import HashingTF, IDF, Normalizer, RegexTokenizer, StopWordsRemover
    texts = catalog.where("text IS NOT NULL").select("file_name", "part", "text")
    index = Pipeline(stages=[RegexTokenizer(inputCol="text", outputCol="tokens", pattern=r"[^a-zA-Z0-9]+"),
                             StopWordsRemover(inputCol="tokens", outputCol="words"),
                             HashingTF(inputCol="words", outputCol="tf", numFeatures=1 << 12),
                             IDF(inputCol="tf", outputCol="tfidf"),
                             Normalizer(inputCol="tfidf", outputCol="vec")]).fit(texts)
    from pyspark.ml.functions import vector_to_array
    parts = index.transform(texts).withColumn("v", vector_to_array("vec", "float32"))
    print(f"\nsearch index over {texts.count()} parts with text")
    for q in ("invoice total", "thank you for shopping", "order problem"):
        qv = index.transform(spark.createDataFrame([(q,)], "text string")).select(
            vector_to_array("vec", "float32").alias("q"))
        top = (parts.crossJoin(qv).select("file_name", "part", F.round(F.vector_inner_product("v", "q").cast("double"), 3).alias("score"))
               .orderBy(F.desc("score"), "file_name", "part").limit(3).collect())
        print(f"  {q:<24}", [(r.file_name, r.part, r.score) for r in top])

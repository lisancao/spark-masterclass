"""Chapter 37: feature vectors for images and audio, exact neighbours, and MLlib's random-projection LSH.

The media library has families of near-copies (resized, moved, brighter, noisy, re-encoded).
labels.csv is the answer key. Every number in the chapter's first half comes from this script.
Run from code/:  uv run python ch37/vectors.py
"""
import io, sys, time, wave
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np
import pandas as pd
from pyspark.ml.feature import BucketedRandomProjectionLSH, StandardScaler, VectorAssembler
from pyspark.ml.functions import array_to_vector, vector_to_array
from pyspark.sql import Window, functions as F
from common.book import session, plan, ROOT

LIB = ROOT / "code" / "data" / "messy" / "media_library"
spark = session("ch37-vectors", spark_ui_showConsoleProgress="false", spark_sql_shuffle_partitions="4")
spark.sparkContext.setLogLevel("OFF")      # the script prints the errors it expects itself


def color_histogram(data, channels, bins=4):
    """Fractions of pixels in each of bins**3 (red, green, blue) cells. The image source stores B, G, R(, A)."""
    px = np.frombuffer(data, dtype=np.uint8).reshape(-1, channels)
    b = g = r = px[:, 0]
    if channels > 1:
        b, g, r = px[:, 0], px[:, 1], px[:, 2]
    step = 256 // bins
    cell = (r // step).astype(int) * bins * bins + (g // step).astype(int) * bins + (b // step).astype(int)
    return (np.bincount(cell, minlength=bins ** 3) / len(cell)).astype(np.float32)


def band_energies(content, bands=16, top_hz=2000.0, by_position=False):
    """Share of a WAV clip's energy in each band. Bands in hertz, or (by_position) by FFT bin number."""
    with wave.open(io.BytesIO(content)) as w:
        rate = w.getframerate()
        x = np.frombuffer(w.readframes(w.getnframes()), dtype="<i2").astype(np.float64) / 32768
    power = np.abs(np.fft.rfft(x)) ** 2
    if by_position:                       # the mistake: bin i means a different frequency at each rate
        band = (np.arange(len(power)) * bands // len(power)).astype(int)
    else:
        band = (np.fft.rfftfreq(len(x), d=1.0 / rate) / (top_hz / bands)).astype(int)
    keep = band < bands
    e = np.bincount(band[keep], weights=power[keep], minlength=bands)
    return (e / e.sum()).astype(np.float32) if e.sum() > 0 else None


@F.pandas_udf("array<float>")
def histogram(data: pd.Series, channels: pd.Series) -> pd.Series:
    return pd.Series([color_histogram(d, c) for d, c in zip(data, channels)])


def bands_udf(by_position):
    @F.pandas_udf("array<float>")
    def bands(content: pd.Series) -> pd.Series:
        return pd.Series([band_energies(c, by_position=by_position) for c in content])
    return bands


labels = spark.read.option("header", True).csv(str(LIB / "labels.csv"))
raw = spark.read.format("image").load(str(LIB / "images"))
print(f"== images: {raw.count()} files, {raw.where('image.nChannels < 0').count()} the image source "
      "could not decode, channels:", sorted(r[0] for r in raw.groupBy("image.nChannels").count().collect()))
images = (raw.where("image.nChannels > 0")
          .select(F.col("_metadata.file_name").alias("file"),
                  histogram("image.data", "image.nChannels").alias("features")))
audio_files = spark.read.format("binaryFile").option("pathGlobFilter", "*.wav").load(str(LIB / "audio"))


def unit(df):
    """Add `unit`: the features scaled to length 1 with the 4.3 Python wrapper of vector_normalize."""
    return df.withColumn("unit", F.vector_normalize("features"))


def exact(df, k=1):
    """Every file against every other file, cosine on unit vectors, best k per file (ties by name)."""
    a = df.select("file", F.col("unit").alias("a"))
    b = df.select(F.col("file").alias("other"), F.col("unit").alias("b"))
    w = Window.partitionBy("file").orderBy(F.desc("cosine"), "other")
    return (a.crossJoin(b).where("file != other")
            .select("file", "other", F.vector_cosine_similarity("a", "b").alias("cosine"))
            .withColumn("rank", F.row_number().over(w)).where(f"rank <= {k}"))


def family_hits(nn):
    fam = labels.select("file", "family")
    j = (nn.where("rank = 1").join(fam, "file")
         .join(fam.select(F.col("file").alias("other"), F.col("family").alias("other_family")), "other"))
    return j.where("family = other_family").count(), j.count()


img = unit(images).cache()
img_nn = exact(img).cache()
hit, n = family_hits(img_nn)
print(f"  nearest image is in the same family for {hit} of {n} images")
print("  misses:", [(r.file, r.other, round(r.cosine, 3)) for r in
                    img_nn.join(labels, "file").join(labels.select(F.col("file").alias("other"),
                                                                    F.col("family").alias("f2")), "other")
                    .where("family != f2").orderBy("file").collect()])

print("\n== audio: features need units")
for by_position in (True, False):
    aud = unit(audio_files.select(F.col("_metadata.file_name").alias("file"),
                                  bands_udf(by_position)("content").alias("features"))
               .where("features IS NOT NULL")).cache()
    hit, n = family_hits(exact(aud))
    label = "bands by FFT bin position" if by_position else "bands in hertz           "
    print(f"  {label}: nearest clip in the same family for {hit} of {n} clips")
    if not by_position:
        audio = aud

print("\n== vectors are arrays of FLOAT for the vector_* functions")
print("  schema:", img.schema["features"].dataType.simpleString(), img.schema["unit"].dataType.simpleString())
probe = spark.createDataFrame([([3.0, 4.0], [4.0, 3.0])], "a array<float>, b array<float>")
print("  ", probe.select(F.vector_norm("a").alias("norm"), F.vector_inner_product("a", "b").alias("dot"),
                         F.round(F.vector_cosine_similarity("a", "b"), 4).alias("cos"),
                         F.vector_l2_distance("a", "b").alias("l2")).first())
for expr, df in (("vector_norm on array<double>", spark.createDataFrame([([3.0, 4.0],)], "a array<double>")),
                 ("vector_inner_product, lengths 2 and 3",
                  spark.createDataFrame([([1.0, 2.0], [1.0, 2.0, 3.0])], "a array<float>, b array<float>"))):
    try:
        col = F.vector_norm("a") if "norm" in expr else F.vector_inner_product("a", "b")
        print(f"  {expr}: {df.select(col).first()[0]}")
    except Exception as e:
        print(f"  {expr}: {type(e).__name__} {str(e).split(chr(10))[0][:80]}")
big = spark.createDataFrame([([3e30, 4e30], [3e30, 4e30])], "a array<float>, b array<float>")
print("  float values near 3e30:", big.select(F.vector_norm("a").alias("norm"),
                                             F.vector_cosine_similarity("a", "b").alias("cos")).first())
print("  vector_avg over the images:", len(img.agg(F.vector_avg("features")).first()[0]), "values")

print("\n== distances on unit vectors: d^2 = 2 - 2 cos")
row = img_nn.join(img.select("file", F.col("unit").alias("u")), "file") \
    .join(img.select(F.col("file").alias("other"), F.col("unit").alias("v")), "other") \
    .select("cosine", F.vector_l2_distance("u", "v").alias("l2")).first()
print(f"  cos {row.cosine:.6f}  l2 {row.l2:.6f}  l2^2 {row.l2 ** 2:.6f}  2-2cos {2 - 2 * row.cosine:.6f}")

print("\n== one column in other units dominates a distance")
sizes = raw.where("image.nChannels > 0").select(F.col("_metadata.file_name").alias("file"),
                                                F.col("_metadata.file_size").cast("double").alias("bytes"))
mixed = img.join(sizes, "file").select("file", array_to_vector("unit").alias("hist"), "bytes")
va = VectorAssembler(inputCols=["hist", "bytes"], outputCol="raw").transform(mixed)
print("  std of file size in bytes:", round(mixed.agg(F.stddev("bytes")).first()[0], 1),
      "| largest histogram value:", round(img.agg(F.max(F.array_max("features"))).first()[0], 3))
for name, col in (("histogram + raw bytes", "raw"), ("after StandardScaler", "scaled")):
    df = va if col == "raw" else StandardScaler(inputCol="raw", outputCol="scaled").fit(va).transform(va)
    df = df.select("file", F.vector_normalize(vector_to_array(col, "float32")).alias("unit"))
    hit, n = family_hits(exact(df))
    print(f"  {name:<22} nearest in the same family: {hit} of {n}")

print("\n== exact search is a cross join")
txt = plan(exact(img), "simple")
print("  operators:", [op for op in ("BroadcastNestedLoopJoin", "CartesianProduct", "WindowGroupLimit", "Window",
                                     "vector_cosine_similarity") if op in txt])

print("\n== MLlib BucketedRandomProjectionLSH: recall at 1 against work")
truth = {r.file: r.cosine for r in img_nn.where("rank = 1").collect()}
vec = img.withColumn("vec", array_to_vector("unit"))
n = vec.count()
print(f"  {n} images, {n * (n - 1)} ordered pairs for exact search")
print("  bucketLength tables  candidates  work   recall@1")
for bl, tables in ((0.05, 1), (0.05, 4), (0.2, 1), (0.2, 4), (0.5, 1), (0.5, 4), (1.0, 4)):
    m = BucketedRandomProjectionLSH(inputCol="vec", outputCol="h", bucketLength=bl,
                                    numHashTables=tables, seed=42).fit(vec)
    h = m.transform(vec).cache()
    c = (m.approxSimilarityJoin(h, h, 2.0).where("datasetA.file != datasetB.file")
         .select(F.col("datasetA.file").alias("file"), F.col("datasetB.file").alias("other"),
                 F.col("distCol").alias("d")))
    best = c.withColumn("r", F.row_number().over(Window.partitionBy("file").orderBy("d", "other"))).where("r = 1")
    found = {r.file: 1 - r.d ** 2 / 2 for r in best.collect()}
    rec = sum(found.get(f, -2) >= cos - 1e-4 for f, cos in truth.items()) / len(truth)
    cand = c.count()
    print(f"  {bl:<12} {tables:<6} {cand:>10}  {cand / (n * (n - 1)):>5.1%}  {rec:>7.3f}")
    h.unpersist()

m = BucketedRandomProjectionLSH(inputCol="vec", outputCol="h", bucketLength=0.2, numHashTables=4, seed=42).fit(vec)
print("  hashes column type:", m.transform(vec).schema["h"].dataType.simpleString())
q = vec.where("file = 'checker_original.png'").first().vec
got = m.approxNearestNeighbors(vec, q, 5).select("file").collect()
print(f"  approxNearestNeighbors(k=5) for checker_original.png returned {len(got)} rows:",
      [r.file for r in got])
try:
    m0 = BucketedRandomProjectionLSH(inputCol="vec", outputCol="h", numHashTables=1).fit(vec)
    print("  no bucketLength: fit and transform worked, rows", m0.transform(vec).count(),
          "| isSet(bucketLength):", m0.isSet("bucketLength"), "| hasDefault:", m0.hasDefault("bucketLength"))
except Exception as e:
    print("  no bucketLength:", type(e).__name__, str(e).split("\n")[0][:90])

print("\n== NEAREST BY: top 3 per image in SQL")
img.select("file", "unit").createOrReplaceTempView("img")
top3 = spark.sql("""
  SELECT q.file, c.file AS other, round(vector_cosine_similarity(q.unit, c.unit), 3) AS cosine
  FROM (SELECT * FROM img WHERE file IN ('banana_yellow_original.png', 'plant_green_original.png')) q
  JOIN (SELECT * FROM img) c
    EXACT NEAREST 3 BY SIMILARITY
      CASE WHEN q.file = c.file THEN -2 ELSE vector_cosine_similarity(q.unit, c.unit) END
""")
top3.orderBy("file", F.desc("cosine"), "other").show(truncate=False)
txt = plan(top3, "simple")
print("  plan operators:", [op for op in ("BroadcastNestedLoopJoin", "CartesianProduct", "ObjectHashAggregate",
                                          "SortAggregate", "HashAggregate", "Generate", "max_by")
                            if op in txt])
for l in txt.splitlines():
    if "max_by" in l or "Generate" in l:
        print("  " + l.strip()[:110])
import re
def ops(sql):
    return [re.sub(r"[^A-Za-z]", "", l.split("[")[0].split("(")[0]) for l in plan(spark.sql(sql), "simple").splitlines()
            if l.strip() and not l.startswith("==")]
same = "SELECT q.file, c.file AS other FROM img q JOIN img c {} NEAREST 3 BY SIMILARITY vector_cosine_similarity(q.unit, c.unit)"
print("  APPROX and EXACT give the same physical operators:", ops(same.format("APPROX")) == ops(same.format("EXACT")))

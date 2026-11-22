"""Chapter 31, part 2: images and audio. The image source and its schema, invalid files, BGR byte
order, the path join trap, an Arrow UDF over pixels, WAV files with the standard library, and
mapInArrow for one file in, many rows out.

Run from code/:  uv run python ch31/media.py
"""
import re
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pyarrow as pa
from pyspark.sql import functions as F
from common.book import session, show, plan, MESSY

spark = session("ch31-media", spark_sql_session_timeZone="UTC")
IMAGES = str(MESSY / "images")
AUDIO = str(MESSY / "audio")


def section(title):
    print(f"\n== {title} ==")


def condition(e):
    m = re.search(r"\[([A-Z_0-9.]+)\]", str(e))
    return m.group(1) if m else f"{type(e).__name__}: {str(e).splitlines()[0][:70]}"


def attempt(label, fn):
    try:
        print(f"  {label:44} -> {fn()}")
    except Exception as e:
        print(f"  {label:44} -> {condition(e)}")


section("the image source")
img = spark.read.format("image").option("recursiveFileLookup", "true").load(IMAGES)
img.printSchema()
show(img.select(F.regexp_replace("image.origin", r"^.*/images/", "").alias("origin"), "image.height", "image.width",
                "image.nChannels", "image.mode", F.length("image.data").alias("data_bytes")).orderBy("origin"))
print("count without dropInvalid:", img.count(),
      " with dropInvalid:", spark.read.format("image").option("recursiveFileLookup", "true")
      .option("dropInvalid", "true").load(IMAGES).count())
print("mode is an OpenCV type code: 0 = CV_8UC1 (gray), 16 = CV_8UC3, 24 = CV_8UC4")

section("pixel bytes are B, G, R (and A)")
red = img.where(F.col("image.origin").contains("red_16x8")).first().image
print("red_16x8.png, first pixel bytes:", bytes(red.data[:3]).hex(" "), " (blue, green, red)")
rgba = img.where(F.col("image.origin").contains("logo_rgba")).first().image
print("logo_rgba_12x12.png, first pixel:", bytes(rgba.data[:4]).hex(" "), " (B, G, R, A)")

section("origin and path are not the same string")
bf = spark.read.format("binaryFile").option("recursiveFileLookup", "true").load(IMAGES)
o = img.select("image.origin").where(F.col("image.origin").contains("red_16x8")).first()[0]
p = bf.select("path").where(F.col("path").contains("red_16x8")).first()[0]
print("image.origin:   ", re.sub(r"/home/[^/]+", "~", o))
print("binaryFile.path:", re.sub(r"/home/[^/]+", "~", p))
print("join on origin = path:", img.join(bf, img["image.origin"] == bf["path"]).count(), "rows")
j = (img.select(F.col("_metadata.file_path").alias("fp"), "image.*")
     .join(bf.select(F.col("_metadata.file_path").alias("fp"), "length"), "fp"))
print("join on _metadata.file_path:", j.count(), "rows")

section("an Arrow UDF over the pixels")
COLOR = pa.struct([("r", pa.float64()), ("g", pa.float64()), ("b", pa.float64())])


@F.arrow_udf("struct<r:double, g:double, b:double>")
def rgb_means(data: pa.Array, channels: pa.Array) -> pa.Array:
    import numpy as np
    out = []
    for d, c in zip(data.to_pylist(), channels.to_pylist()):
        if not d or c < 1:
            out.append(None)
            continue
        px = np.frombuffer(d, dtype=np.uint8).reshape(-1, c).astype(float)
        out.append({"r": px[:, 0].mean(), "g": px[:, 0].mean(), "b": px[:, 0].mean()} if c == 1 else
                   {"r": px[:, 2].mean(), "g": px[:, 1].mean(), "b": px[:, 0].mean()})
    return pa.array(out, type=COLOR)


means = (img.where("image.nChannels > 0")
         .select(F.col("_metadata.file_name").alias("file"), rgb_means("image.data", "image.nChannels").alias("c"))
         .select("file", F.round("c.r", 1).alias("r"), F.round("c.g", 1).alias("g"), F.round("c.b", 1).alias("b")))
show(means.orderBy("file"))
print("plan:", next(l.strip()[:70] for l in plan(means, "simple").splitlines() if "Python" in l))

section("WAV files with the standard library")


def wav_info(content):
    import io
    import wave
    import numpy as np
    with wave.open(io.BytesIO(content)) as w:
        rate, ch, n = w.getframerate(), w.getnchannels(), w.getnframes()
        s = np.frombuffer(w.readframes(n), dtype="<i2").astype(float)
    return (rate, ch, n / rate, float(np.sqrt((s ** 2).mean()) / 32768) if len(s) else 0.0)


info = F.udf(wav_info, "struct<rate:int, channels:int, seconds:double, rms:double>")
wav = spark.read.format("binaryFile").option("pathGlobFilter", "*.wav").load(AUDIO)
print("RIFF header of tone_440.wav:", wav.where("path LIKE '%tone_440%'").first().content[:16])
show(wav.select(F.col("_metadata.file_name").alias("file"), info("content").alias("w"))
     .select("file", "w.rate", "w.channels", "w.seconds", F.round("w.rms", 4).alias("rms")).orderBy("file"))

section("mapInArrow: one file in, one row for each 50 ms window out")


def windows(batches, int_type=pa.int32()):
    import io
    import wave
    import numpy as np
    for batch in batches:
        files, idx, peaks = [], [], []
        for name, content in zip(batch.column("file").to_pylist(), batch.column("content").to_pylist()):
            with wave.open(io.BytesIO(content)) as w:
                rate = w.getframerate()
                s = np.frombuffer(w.readframes(w.getnframes()), dtype="<i2")
            step = rate * 50 // 1000
            for i in range(0, len(s), step):
                files.append(name)
                idx.append(i // step)
                peaks.append(float(np.abs(s[i:i + step].astype(int)).max()) / 32768)
        yield pa.RecordBatch.from_arrays([pa.array(files), pa.array(idx, int_type), pa.array(peaks)],
                                         names=["file", "window", "peak"])


src = wav.select(F.col("_metadata.file_name").alias("file"), "content")
win = src.mapInArrow(windows, "file string, window int, peak double")
show(win.groupBy("file").agg(F.count("*").alias("windows"), F.round(F.max("peak"), 3).alias("max_peak")).orderBy("file"))
attempt("the same, with int64 for an int column", lambda: src.mapInArrow(
    lambda it: windows(it, pa.int64()), "file string, window int, peak double").count())

section("streaming binaryFile needs a schema")
attempt("readStream.format('binaryFile') without a schema",
        lambda: spark.readStream.format("binaryFile").load(AUDIO).schema.simpleString())

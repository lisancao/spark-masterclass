"""Chapter 48: a paginated REST API as a Python data source: pages as partitions, retries, VARIANT.

Run from code/:  uv run python ch48/api_source.py
"""
import json, math, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import functions as F
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from common.book import session
from tickets_api import TicketsAPI

api = TicketsAPI()
spark = session("ch48-api")

# 1. The first version everyone writes: fetch every page on the driver.
import urllib.request
rows = []
try:
    for page in range(5):
        with urllib.request.urlopen(f"{api.url}?page={page}", timeout=10) as r:
            rows += json.load(r)["items"]
except Exception as e:
    print(f"driver loop: {len(rows)} rows, then {type(e).__name__}: {e}")
print("driver loop:", api.take_log(), "\n")
api.flaky_hits = 0          # from here on, every odd request to page 3 fails


def fetch_json(url, timeout, retries):
    """GET with a timeout. Retry 5xx and network errors with a growing pause, raise 4xx at once."""
    import json, time
    from urllib.error import HTTPError, URLError
    from urllib.request import urlopen

    for attempt in range(retries + 1):
        try:
            with urlopen(url, timeout=timeout) as response:
                return json.load(response)
        except HTTPError as e:           # a subclass of URLError, so it must come first
            if e.code < 500 or attempt == retries:
                raise                    # 4xx: our request is wrong, a retry will not help
        except URLError:
            if attempt == retries:
                raise
        time.sleep(0.1 * 2 ** attempt)


class PagePartition(InputPartition):
    def __init__(self, page):
        self.page = page


class TicketsDataSource(DataSource):
    @classmethod
    def name(cls):
        return "tickets"

    def __init__(self, options):
        super().__init__(options)
        if "url" not in options:
            raise ValueError("option 'url' is required")

    def schema(self):
        return "id int, status string, subject string, meta variant"

    def reader(self, schema):
        return TicketsReader(self.options)


class TicketsReader(DataSourceReader):
    def __init__(self, options):
        self.url = options["url"]
        self.page_size = int(options.get("page_size", "20"))
        self.timeout = float(options.get("timeout", "10"))
        self.retries = int(options.get("retries", "3"))

    def page_url(self, page):
        return f"{self.url}?page={page}&page_size={self.page_size}"

    def partitions(self):
        first = fetch_json(self.page_url(0), self.timeout, self.retries)
        return [PagePartition(p) for p in range(math.ceil(first["total"] / self.page_size))]

    def read(self, partition):
        import json
        from pyspark.sql.types import VariantVal

        body = fetch_json(self.page_url(partition.page), self.timeout, self.retries)
        for item in body["items"]:
            yield (item["id"], item["status"], item["subject"],
                   VariantVal.parseJson(json.dumps(item["meta"])))


spark.dataSource.register(TicketsDataSource)

if "--task-retries" in sys.argv:
    # Run with SPARK_MASTER='local[4,3]': up to 3 attempts per task, and no retries in the reader.
    n = spark.read.format("tickets").option("url", api.url).option("retries", 0).load().count()
    print(f"master {spark.sparkContext.master}, retries=0: count() = {n}:", api.take_log())
    sys.exit(0)


def error_lines(e):
    """The condition line and the last Python exception line of an error, without module paths."""
    import re
    text = str(e).splitlines()
    get = getattr(e, "getCondition", None)
    cond = get() if get else None
    head = re.sub(r":? Traceback.*", "", text[0])[:80]
    if cond and cond not in head:
        head = f"[{cond}] {head}"
    hits = [m.group(1) for l in text
            if (m := re.match(r"^\s*(?:[\w.]+\.)?(\w+(?:Error|Exception): .*)", l))]
    return head, (hits[-1][:88] if hits else "")


# 2. A missing option fails while Spark analyzes the plan, in DataSource.__init__.
try:
    spark.read.format("tickets").load()
except Exception as e:
    head, last = error_lines(e)
    print("no url:", head, "\n       ", last, "\n")

# With a user schema, Spark has no reason to create the DataSource at load(), so the check waits.
user = spark.read.format("tickets").schema("id int, status string, subject string, meta variant").load()
print("no url, user schema: load() returned a DataFrame with columns", user.columns)
try:
    user.count()
except Exception as e:
    head, last = error_lines(e)
    print("  first action:", head, "\n               ", last, "\n")

# 3. Count the calls that each step makes.
df = spark.read.format("tickets").option("url", api.url).load()
print("load():", api.take_log())
df.explain()
print("explain():", api.take_log())
print(f"count() = {df.count()} in {df.rdd.getNumPartitions()} partitions:", api.take_log())
df.groupBy("status").count().orderBy("status").show()
print("groupBy().count():", api.take_log())

df.cache()
df.count()
print("cache() + count():", api.take_log())
df.where("status = 'open'").count()
df.select(F.max("id")).collect()
print("two more queries on the cached df:", api.take_log())
df.unpersist()

# 4. The loose part as VARIANT.
print()
(df.select(F.explode(F.try_variant_get("meta", "$.tags", "array<string>")).alias("tag"))
   .groupBy("tag").count().orderBy("tag").show())
(df.groupBy(F.try_variant_get("meta", "$.priority", "string").alias("priority"))
   .count().orderBy("priority").show())
gold = df.filter(F.try_variant_get("meta", "$.customer.tier", "string") == "gold").count()
print("gold customers:", gold, " tickets without a customer:",
      df.filter(F.try_variant_get("meta", "$.customer", "string").isNull()).count())
api.take_log()

# 5. Bigger pages, fewer partitions; no retries, one failure.
big = spark.read.format("tickets").option("url", api.url).option("page_size", 50).load()
print(f"\npage_size=50: {big.count()} rows in {big.rdd.getNumPartitions()} partitions:", api.take_log())
try:
    spark.read.format("tickets").option("url", api.url).option("retries", 0).load().count()
except Exception as e:
    head, last = error_lines(e)
    print("retries=0:", head, "\n          ", last)
print("retries=0:", api.take_log())
print("duplicate ids:", df.groupBy("id").count().where("count > 1").count())


# 6. A dict in a VARIANT column.
class DictVariant(TicketsDataSource):
    @classmethod
    def name(cls):
        return "dictvariant"

    def reader(self, schema):
        class R(TicketsReader):
            def read(self, partition):
                yield (1, "open", "x", {"tags": []})
        return R(self.options)


spark.dataSource.register(DictVariant)
try:
    spark.read.format("dictvariant").option("url", api.url).load().count()
except Exception as e:
    print("dict for a variant column:", *error_lines(e), sep="\n  ")
api.server.shutdown()

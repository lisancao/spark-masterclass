"""Figures for Chapter 12. Numbers and names from code/ch12/output/catalogs.txt,
tables_views.txt and functions.txt. Run: FIG_CHECK_DIR=<scratch> python3 code/ch12/figures/make_figures.py"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from svgfig import Fig

# 1. The session in catalogs.py: two catalogs, their namespaces and tables (catalogs.txt).
f = Fig(900, 360, "Catalogs, namespaces and tables in one session")
f.text(20, 28, "One session, two catalogs: catalog.namespace.table", 15, 700, anchor="start")
f.box(330, 46, 240, 50, "SparkSession", "current: spark_catalog.taxi", kind="ink", size=15, sub_mono=True)
# spark_catalog
f.box(60, 140, 300, 54, "spark_catalog", "session catalog (in-memory)", kind="acc", size=15, mono=True)
f.box(40, 230, 150, 44, "default", kind="n", mono=True)
f.box(210, 230, 170, 44, "taxi", kind="n", mono=True)
f.box(210, 296, 170, 44, "zones", "parquet, MANAGED", kind="blue", mono=True, size=13)
# h2
f.box(540, 140, 300, 54, "h2", "JDBCTableCatalog over H2", kind="purple", size=15, mono=True)
for i, ns in enumerate(["INFORMATION_SCHEMA", "PUBLIC", "ops"]):
    f.box(500 + i * 135, 230, 128, 44, ns, kind="n", mono=True, size=11.5 if len(ns) > 10 else 13)
f.box(770, 296, 128, 44, "zone_owner", "H2 table", kind="blue", mono=True, size=13)
for x1, y1, x2, y2 in [(450, 96, 210, 140), (450, 96, 690, 140), (210, 194, 115, 230), (210, 194, 295, 230),
                       (295, 274, 295, 296), (690, 194, 564, 230), (690, 194, 699, 230), (690, 194, 834, 230),
                       (834, 274, 834, 296)]:
    f.arrow(x1, y1, x2, y2)
f.code(420, 296, 330, ["SELECT ... FROM spark_catalog.taxi.zones",
                       "  JOIN h2.ops.zone_owner USING (LocationID)"], size=11, title="one query")
f.text(450, 128, "SHOW CATALOGS lists h2 only after first use", 11.5, 600, c="mute")
f.save("ch12-catalogs")

# 2. DROP TABLE: managed against external (tables_views.txt, section 6).
f = Fig(900, 300, "DROP TABLE, managed and external")
f.text(20, 28, "DROP TABLE removes the catalog entry. For a managed table, it also removes the files.",
       14, 700, anchor="start")
for j, (name, typ, loc, after, k) in enumerate([
        ("daily", "MANAGED", "warehouse/taxi.db/daily", "gone, 0 parquet files", "bad"),
        ("daily_ext", "EXTERNAL", "external/daily_copy", "exists, 1 parquet files", "ok")]):
    x = 30 + j * 440
    f.box(x, 56, 190, 56, f"taxi.{name}", f"Type: {typ}", kind="acc", mono=True, size=14)
    f.box(x + 220, 56, 190, 56, "1 parquet file", loc, kind="n", size=13, sub_mono=True, subsize=10.5)
    f.arrow(x + 190, 84, x + 220, 84)
    f.text(x + 205, 150, "DROP TABLE", 13, 700, mono=True)
    f.arrow(x + 205, 160, x + 205, 190)
    f.box(x, 200, 190, 56, "catalog entry", "removed", kind="bad", size=13)
    f.box(x + 220, 200, 190, 56, "files", after, kind=k, size=13)
f.save("ch12-drop-table")

# 3. The built-in functions by group (functions.txt, section 3).
groups = [("agg_funcs", 87), ("datetime_funcs", 85), ("string_funcs", 76), ("math_funcs", 68),
          ("sketch_funcs", 40), ("misc_funcs", 27), ("array_funcs", 25), ("predicate_funcs", 21),
          ("variant_funcs", 17), ("conversion_funcs", 14), ("bitwise_funcs", 13), ("xml_funcs", 12),
          ("lambda_funcs", 12), ("conditional_funcs", 11), ("window_funcs", 10), ("map_funcs", 9),
          ("generator_funcs", 7), ("hash_funcs", 7), ("vector_funcs", 7), ("json_funcs", 7),
          ("collection_funcs", 6), ("url_funcs", 5), ("st_funcs", 5), ("avro_funcs", 3), ("csv_funcs", 3),
          ("protobuf_funcs", 2), ("struct_funcs", 2)]
f = Fig(900, 470, "Built-in functions by group")
f.text(20, 28, "581 built-in scalar, aggregate and window functions in 4.3.0, by group", 15, 700, anchor="start")
half = 14
for i, (g, n) in enumerate(groups):
    col, row = divmod(i, half)
    x, y = 20 + col * 440, 50 + row * 29
    new = g in ("sketch_funcs", "variant_funcs", "vector_funcs", "st_funcs")
    f.text(x + 140, y + 17, g, 12.5, 600 if new else 500, anchor="end", mono=True, c="acc" if new else "fg")
    w = 250 * n / 87
    f.rect(x + 150, y + 4, w, 18, "acc" if new else "blue", r=3)
    f.text(x + 156 + w, y + 18, str(n), 11.5, 600, anchor="start", c="mute")
f.text(20, 462, "orange = groups that hold types or features added in 4.x (VARIANT, vectors, sketches, geospatial)",
       11, 400, anchor="start", c="mute", italic=True)
f.save("ch12-function-groups")

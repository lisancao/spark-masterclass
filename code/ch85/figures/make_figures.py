"""Figures for Chapter 85. Values from code/ch85/output/serde_tables.txt and derby_rows.txt.
Run: FIG_CHECK_DIR=<scratch> python3 code/ch85/figures/make_figures.py"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from svgfig import Fig

# 1. SerDe tables against data source tables: metastore entry -> reader and writer (serde_tables.txt 2, 4).
ROWS = [
    ("z_text", "STORED AS TEXTFILE", "hive", "LazySimpleSerDe", "Scan hive", "InsertIntoHiveTable", "acc"),
    ("z_csv_serde", "ROW FORMAT SERDE", "hive", "OpenCSVSerde", "Scan hive", "InsertIntoHiveTable", "acc"),
    ("z_hive_pq", "STORED AS PARQUET", "hive", "ParquetHiveSerDe", "FileScan parquet", "InsertIntoHadoopFsRelationCommand", "blue"),
    ("z_hive_orc", "STORED AS ORC", "hive", "OrcSerde", "FileScan orc", "InsertIntoHadoopFsRelationCommand", "blue"),
    ("z_ds_pq", "USING parquet", "parquet", "ParquetHiveSerDe", "FileScan parquet", "InsertIntoHadoopFsRelationCommand", "ok"),
    ("z_default", "(no clause)", "parquet", "ParquetHiveSerDe", "FileScan parquet", "InsertIntoHadoopFsRelationCommand", "ok"),
]
f = Fig(960, 430, "Hive SerDe tables and data source tables")
f.text(20, 30, "What the metastore keeps, and the reader and writer that Spark picks", 15, 700, anchor="start")
heads = [(20, "table and clause"), (250, "Provider / Serde Library"), (500, "scan node"), (680, "write command")]
for x, h in heads:
    f.text(x, 62, h, 12.5, 700, anchor="start", c="mute")
for i, (t, clause, prov, serde, scan, write, k) in enumerate(ROWS):
    y = 74 + i * 54
    f.box(20, y, 210, 46, t, clause, kind="n", mono=True, size=13, sub_mono=True)
    f.box(250, y, 220, 46, prov, serde, kind="purple" if prov == "hive" else "ok", mono=True, size=13, sub_mono=True)
    f.arrow(470, y + 23, 500, y + 23)
    f.box(500, y, 160, 46, scan, kind=k, mono=True, size=13)
    f.box(680, y, 260, 46, write, kind=k, mono=True, size=11.5 if len(write) > 25 else 13)
f.text(20, 410, "orange: the Hive SerDe reads and writes each row.  blue: a Hive table that Spark converts "
       "(convertMetastoreParquet/Orc).  green: a data source table.", 11.5, 400, anchor="start", c="mute", italic=True)
f.save("ch85-serde-paths")

# 2. Metastore rows (derby_rows.txt) and the folders that they point to.
f = Fig(960, 480, "Rows in the metastore database")
f.text(20, 30, "metastore_db after partitions.py: tables of the Hive Metastore schema, rows from derby_rows.txt",
       14, 700, anchor="start")
f.table(20, 50, [70, 110, 140], [["DBS", "TBLS", "TBL_TYPE"], ["taxi", "zones", "MANAGED_TABLE"],
        ["taxi", "zones_ds", "MANAGED_TABLE"], ["taxi", "zones_txt", "MANAGED_TABLE"]],
        mono_cols=(0, 1, 2), size=12, rh=26)
f.table(360, 50, [100, 250, 220], [["TBLS", "SDS.INPUT_FORMAT (class)", "SERDES.SLIB (class)"],
        ["zones", "MapredParquetInputFormat", "ParquetHiveSerDe"],
        ["zones_ds", "MapredParquetInputFormat", "ParquetHiveSerDe"],
        ["zones_txt", "TextInputFormat", "LazySimpleSerDe"]], mono_cols=(0, 1, 2), size=12, rh=26)
parts = [("zones", "borough=Bronx"), ("zones", "borough=EWR"), ("zones", "borough=Manhattan"),
         ("zones", "borough=Queens"), ("zones_ds", "borough=EWR"), ("zones_txt", "borough=Bronx"),
         ("zones_txt", "borough=EWR"), ("zones_txt", "borough=Queens")]
f.text(20, 178, "PARTITIONS (each with its own SDS row and LOCATION)", 12.5, 700, anchor="start", c="mute")
f.table(20, 188, [100, 160, 260], [["TBL", "PART_NAME", "SDS.LOCATION"]] +
        [[t, p, f"taxi.db/{t}/{p}"] for t, p in parts], mono_cols=(0, 1, 2), size=12, rh=26)
f.text(580, 178, "TABLE_PARAMS of zones_ds (Spark keys)", 12.5, 700, anchor="start", c="mute")
f.table(580, 188, [250, 110], [["PARAM_KEY", "PARAM_VALUE"], ["spark.sql.sources.provider", "parquet"],
        ["spark.sql.sources.schema", "{\"type\":...}"], ["spark.sql.partitionProvider", "catalog"],
        ["spark.sql.create.version", "4.3.0"]], mono_cols=(0, 1), size=12, rh=26, hi_rows=(1, 2))
f.box(580, 372, 360, 64, "warehouse/taxi.db/", "zones/borough=Bronx/part-...parquet  ...", kind="ok", mono=True,
      size=13, sub_mono=True)
f.arrow(550, 330, 580, 380)
f.text(600, 362, "each LOCATION is a folder here", 11.5, 400, anchor="start", c="mute")
f.text(20, 468, "The metastore keeps names, formats and locations. The rows of data are only in the files.",
       12, 400, anchor="start", c="mute", italic=True)
f.save("ch85-metastore-rows")

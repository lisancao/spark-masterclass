"""Figure for Chapter 10, Step 17. Conditions and SQLSTATE codes from code/ch10/output/errors.txt (sections 1 and 4)."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "common"))
from svgfig import Fig

f = Fig(800, 375, "SQLSTATE codes of the errors in errors.py")
# anatomy of one code
f.text(20, 30, "A SQLSTATE is a class and a subclass", 15, 700, anchor="start")
f.box(30, 46, 90, 60, "22", "class", kind="blue", size=18, mono=True)
f.box(124, 46, 110, 60, "012", "subclass", kind="n", size=18, mono=True)
f.text(255, 70, "22 = data exception", 13, 600, anchor="start", c="blue")
f.text(255, 90, "22012 = division by zero (from the SQL standard)", 13, 400, anchor="start")
f.text(630, 70, "42 = syntax error or", 13, 600, anchor="start", c="acc")
f.text(630, 88, "access rule violation", 13, 600, anchor="start", c="acc")
rows = [["condition", "SQLSTATE", "error-states.json description", "cause"],
        ["DIVIDE_BY_ZERO", "22012", "division by zero", "data"],
        ["CAST_INVALID_INPUT", "22018", "invalid character value for cast", "data"],
        ["UNRESOLVED_COLUMN.WITH_SUGGESTION", "42703", "An undefined column or parameter name ...", "code"],
        ["PARSE_SYNTAX_ERROR", "42601", "A character, token, or clause is invalid ...", "code"],
        ["TABLE_OR_VIEW_NOT_FOUND", "42P01", "undefined table", "code or setup"],
        ["NOT_EXPECTED_TYPE (Python side)", "42K09", "Data type mismatch", "code"],
        ["(none): sample(fraction=2.0)", "None", "no condition, no SQLSTATE", "read the message"]]
f.table(20, 122, [262, 78, 300, 120], rows, rh=28, size=12.5, mono_cols=(0, 1), ok_rows=(), hi_rows=(3, 4, 5, 6),
        hi_kind="acc", bad_rows=(7,))
for i in (1, 2):
    f.rect(20, 122 + i * 28, 760, 28, "blue", r=0, sw=0)
f.table(20, 122, [262, 78, 300, 120], rows, rh=28, size=12.5, mono_cols=(0, 1))
f.text(780, 366, "Spark 4.3.0, Classic and Connect gave the same codes", 11, 400, anchor="end", c="mute", italic=True)
f.save("ch10-sqlstate")

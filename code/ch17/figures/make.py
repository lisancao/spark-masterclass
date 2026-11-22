"""Chapter 17 figures. Numbers from code/ch17/output/two_errors.txt, define.txt and grains.txt."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.figkit import Fig  # noqa: E402

CHECK = Path(sys.argv[1]) if len(sys.argv) > 1 else None


def ratios():
    rows = [("Manhattan", 3266362, 7.0777), ("Queens", 309265, 4.5503), ("Brooklyn", 99965, 6.1579),
            ("Bronx", 13816, 5.138), ("Unknown", 3656, 5.3713), ("N/A", 983, 12.6907),
            ("Staten Island", 209, 4.1866), ("EWR", 131, 42.1885)]
    f = Fig(900, 380, "Fare per mile by borough, the average of the ratios and the ratio of the totals")
    f.text(20, 26, "fare_per_mile for each pickup borough (June 2026)", size=14, weight=700)
    X0, S = 300, 13.5   # px per dollar per mile
    f.text(185, 44, "trips", size=11, c="mute", anchor="end")
    f.text(265, 44, "ratio", size=11, c="mute", anchor="end")
    for i, (b, n, r) in enumerate(rows):
        y = 50 + i * 34
        f.text(20, y + 17, b, size=13)
        f.text(185, y + 17, f"{n:,}", size=11, anchor="end", c="mute", mono=True)
        f.text(265, y + 17, f"{r}", size=12, anchor="end", mono=True)
        f.rect(X0, y + 2, r * S, 22, c="si", fill=0.3, rx=3, stroke=0)
    yb = 50 + 8 * 34 + 4
    for v, c, lab, dy in ((10.9201, "bad", "average of the 8 ratios: 10.9201 (wrong)", 0),
                          (6.3158, "ok", "total fare / total distance: 6.3158", 16)):
        x = X0 + v * S
        f.line(x, 40, x, yb, c=c, w=2, dash="6 4")
        f.text(x + 6, yb + 16 + dy, lab, size=12, c=c, weight=600)
    f.text(20, 372, "Each borough has the same weight in an average: EWR (131 trips) counts as much as Manhattan.",
           size=12, c="mute")
    return f.save("ch17-ratios", CHECK)


def stored():
    f = Fig(900, 330, "What the catalog stores for a metric view")
    f.text(20, 26, "YAML body", size=14, weight=700)
    f.text(470, 26, "Catalog: column metadata of taxi.trip_metrics", size=14, weight=700)
    yaml = [("source: taxi.trips", "gr"), ("filter: pickup_borough NOT IN (...)", "gr"),
            ("dimensions:", "si"), ("  borough: pickup_borough", "si"), ("  pickup_hour: hour(pickup_ts)", "si"),
            ("measures:", "go"), ("  trips: COUNT(1)", "go"),
            ("  fare_per_mile: SUM(fare_amount) / SUM(trip_distance)", "go"),
            ("  active_zones: COUNT(DISTINCT pickup_zone_id)", "go")]
    for i, (t, c) in enumerate(yaml):
        ind = 16 if t.startswith("  ") else 0
        f.text(24 + ind, 56 + i * 24, t.strip(), size=12, mono=True, c=c if t.endswith(":") else "ink",
               weight=700 if t.endswith(":") else 400)
    cols = [("borough", "string", "dimension", "si"), ("pickup_hour", "int", "dimension", "si"),
            ("trips", "bigint", "measure", "go"), ("fare_per_mile", "double", "measure", "go"),
            ("active_zones", "bigint", "measure", "go")]
    f.table(470, 40, [140, 70, 110, 100], [(a, b, c, "") for a, b, c, _ in cols],
            head=["column", "type", "metric_view.type", ""], size=12, rowh=26,
            hi={i: c for i, (_, _, _, c) in enumerate(cols)})
    f.text(470, 222, "Table properties:", size=12, weight=600)
    f.text(470, 240, "metric_view.from.name = taxi.trips", size=12, mono=True)
    f.text(470, 258, "metric_view.where = pickup_borough NOT IN ('Unknown', 'N/A')", size=11, mono=True)
    f.text(470, 282, "Type: METRIC_VIEW. View Text: the full YAML.", size=12)
    f.text(20, 318, "The type of each measure comes from the analyzer when Spark creates the view. Selected rows of define.txt.",
           size=12, c="mute")
    return f.save("ch17-stored", CHECK)


def distinct():
    f = Fig(900, 210, "The sum of daily distinct counts against the distinct count of the month")
    f.text(20, 26, "Active pickup zones in June 2026", size=14, weight=700)
    # 30 daily bars of average 236.1 (schematic heights: the daily counts are not in the output)
    for d in range(30):
        f.rect(20 + d * 19, 60, 15, 50, c="bad", fill=0.3, rx=2, stroke=0)
    f.text(20, 132, "30 daily distinct counts, average 236.1: the sum is 7083 (wrong)", size=12, c="bad", weight=600)
    f.rect(630, 50, 60, 60, c="ok", fill=0.3, rx=3, stroke=0)
    f.text(630, 132, "distinct in June: 258", size=12, c="ok", weight=600)
    f.text(20, 170, "A zone with pickups on 30 days is in 30 daily counts. Equal bar heights: schematic,",
           size=12, c="mute")
    f.text(20, 186, "the output has only the sum and the average of the daily counts.", size=12, c="mute")
    return f.save("ch17-distinct", CHECK)


if __name__ == "__main__":
    for fn in (ratios, stored):
        print(fn())

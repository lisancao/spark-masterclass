"""Builds shop.db, a SQLite file with a messy `orders` table (Chapters 48 and 49). Driver side only.

order_id is an INTEGER PRIMARY KEY: ids 1 to 1201, plus six legacy ids from 100001.
The other columns hold the kinds of values that real tables collect over the years.
"""
import random
import sqlite3

DDL = """CREATE TABLE orders (
    order_id  INTEGER PRIMARY KEY,
    customer  TEXT,
    status    VARCHAR(16),
    amount    DECIMAL(10,2),
    qty       INT,
    placed_at DATETIME,
    gift      BOOLEAN,
    note
)"""

CUSTOMERS = ["Ana", "ANA", "ana", "Bo", "Chen", "Dara", "O'Brien", "x' OR '1'='1", "Eve", "Femi"]
STATUSES = ["shipped", "shipped", "Shipped", "pending", "cancelled", "SHIPPED", "returned"]


def build(path):
    rnd = random.Random(47)
    conn = sqlite3.connect(path)
    conn.execute("DROP TABLE IF EXISTS orders")
    conn.execute(DDL)
    ids = list(range(1, 1202)) + list(range(100001, 100007))
    rows = []
    for i in ids:
        amount = round(rnd.uniform(1, 250), 2)
        r = rnd.random()
        if r < 0.03:
            amount = "N/A"
        elif r < 0.05:
            amount = ""
        elif r < 0.06:
            amount = "12,50"
        elif r < 0.075:
            amount = None
        qty = rnd.randint(1, 6)
        if rnd.random() < 0.03:
            qty = rnd.choice(["two", "4 ", 2.5])
        day = 1 + i % 28
        fmt = i % 3
        placed = (f"2026-09-{day:02d} 07:00:00" if fmt == 0 else
                  f"2026-09-{day:02d}T07:00:00Z" if fmt == 1 else
                  1788246000 + day * 86400)
        gift = rnd.choice([0, 1, "yes", "no", "true", None])
        note = rnd.choice([None, None, "fragile", 42, "42", "leave at door"])
        customer = CUSTOMERS[i % len(CUSTOMERS)]
        if customer == "x' OR '1'='1" and i != 777:
            customer = "Gus"                    # only order 777 has that name
        rows.append((i, customer, rnd.choice(STATUSES), amount, qty, placed, gift, note))
    conn.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
    conn.commit()
    conn.close()
    return len(rows)

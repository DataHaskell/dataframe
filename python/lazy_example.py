"""
Example: streaming scans with hyrax's lazy engine.

Run from the repo root:

    cabal build dataframe-arrow
    python3 -m venv .venv && source ./.venv/bin/activate
    pip install pyarrow
    python3 python/lazy_example.py
"""

import os
import random
import time

import hyrax as hx


# ---------------------------------------------------------------------------
# Synthesize a CSV the first time we run.
# ---------------------------------------------------------------------------

CSV_PATH = os.environ.get("HYRAX_DEMO_CSV", "/tmp/hyrax_demo_orders.csv")
N_ROWS = int(os.environ.get("HYRAX_DEMO_ROWS", "100000"))

CATEGORIES = ["electronics", "books", "kitchen", "garden", "toys", "office", "sports"]
COUNTRIES = ["US", "GB", "DE", "FR", "JP", "BR", "IN", "CA", "AU", "ZA"]


def generate_csv(path: str, n: int) -> None:
    print(f"Generating {n:,} rows of synthetic order data → {path}")
    random.seed(42)
    t0 = time.perf_counter()
    with open(path, "w") as f:
        f.write("order_id,customer_id,country,category,quantity,unit_price,timestamp,promo_code,session_id,referrer\n")
        for i in range(n):
            country = random.choice(COUNTRIES)
            category = random.choice(CATEGORIES)
            qty = random.randint(1, 9)
            price = round(random.uniform(2.0, 999.0), 2)
            ts = 1_700_000_000 + i
            promo = "" if random.random() < 0.7 else f"P{random.randint(1, 50):03d}"
            session = f"s{random.randint(0, 999_999)}"
            ref = random.choice(["search", "direct", "social", "email", "ad"])
            f.write(
                f"{i},c{random.randint(0, 99_999)},{country},{category},"
                f"{qty},{price},{ts},{promo},{session},{ref}\n"
            )
    size_mb = os.path.getsize(path) / 1_048_576
    print(f"  done in {time.perf_counter() - t0:.1f}s, {size_mb:.0f} MB on disk")


if not os.path.exists(CSV_PATH) or os.path.getsize(CSV_PATH) < N_ROWS * 30:
    generate_csv(CSV_PATH, N_ROWS)
else:
    print(f"Reusing existing {CSV_PATH}")


t0 = time.perf_counter()
revenue_by_country = (
    hx.scan_csv(CSV_PATH, {
        "country":    "text",
        "quantity":   "int",
        "unit_price": "double",
    })
    .filter(hx.col("quantity") >= 3)
    .derive("revenue", hx.col("quantity") * hx.col("unit_price"))
    .groupBy(["country"])
    .aggregate({
        "n":          hx.count(hx.col("country")),
        "total_rev":  hx.sum(hx.col("revenue")),
        "max_rev":    hx.max(hx.col("revenue")),
        "median_rev": hx.median(hx.col("revenue")),
    })
)
elapsed = time.perf_counter() - t0
print(f"\n  ran in {elapsed:.2f}s on {N_ROWS:,} rows (3/10 columns parsed)\n")

ranked = (
    hx.from_arrow(revenue_by_country)
      .sort(["total_rev"], ascending=False)
      .collect()
      .to_pylist()
)
print(f"  {'country':<8}  {'orders':>8}  {'total_rev':>14}  {'max_rev':>10}  {'median_rev':>11}")
for row in ranked:
    print(
        f"  {row['country']:<8}  {row['n']:>8,}  "
        f"{row['total_rev']:>14,.2f}  "
        f"{row['max_rev']:>10,.2f}  "
        f"{row['median_rev']:>11,.2f}"
    )

t0 = time.perf_counter()
by_category = (
    hx.scan_csv(CSV_PATH, {
        "category":   "text",
        "quantity":   "int",
        "unit_price": "double",
    })
    .derive("revenue", hx.col("quantity") * hx.col("unit_price"))
    .groupBy(["category"])
    .aggregate({
        "n":         hx.count(hx.col("category")),
        "mean_qty":  hx.mean(hx.col("quantity")),
        "total_rev": hx.sum(hx.col("revenue")),
    })
)
elapsed = time.perf_counter() - t0
print(f"\n  ran in {elapsed:.2f}s\n")

ranked = (
    hx.from_arrow(by_category)
      .sort(["total_rev"], ascending=False)
      .collect()
      .to_pylist()
)
print(f"  {'category':<12}  {'orders':>8}  {'mean_qty':>9}  {'total_rev':>14}")
for row in ranked:
    print(
        f"  {row['category']:<12}  {row['n']:>8,}  "
        f"{row['mean_qty']:>9.2f}  "
        f"{row['total_rev']:>14,.2f}"
    )

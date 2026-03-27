"""
M1.3 — Data Integrity & Validation
====================================
Three checks:
  1. CSV line counts vs DuckDB row counts
  2. NULL analysis on days_since_prior_order (expected for first orders)
  3. Orphaned records (product_id / order_id referential integrity)

Run with:
    .venv/Scripts/python scripts/validate.py
"""

import os
import sys

import duckdb

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB   = os.path.join(ROOT, "models", "instacart.db")
DATA = os.path.join(ROOT, "data")

if not os.path.exists(DB):
    print(f"ERROR: Database not found at {DB}")
    print("Run scripts/build_database.py first.")
    sys.exit(1)

con = duckdb.connect(DB, read_only=True)

PASS = "PASS"
WARN = "WARN"
FAIL = "FAIL"

issues: list[str] = []

def banner(msg: str) -> None:
    print(f"\n{'-'*60}\n{msg}\n{'-'*60}")

def result(label: str, status: str, detail: str = "") -> None:
    icon = {"PASS": "OK", "WARN": "!!", "FAIL": "XX"}.get(status, "?")
    suffix = f"  → {detail}" if detail else ""
    print(f"  [{icon}] {label}{suffix}")
    if status in (WARN, FAIL):
        issues.append(f"[{status}] {label}: {detail}")

# ══════════════════════════════════════════════════════════════════════════════
# Check 1 — CSV line counts vs DuckDB row counts
# ══════════════════════════════════════════════════════════════════════════════
banner("Check 1 — CSV line counts vs DuckDB row counts")

CSV_TABLE_MAP = {
    "aisles.csv":                  "aisles",
    "departments.csv":             "departments",
    "products.csv":                "products",
    "orders.csv":                  "orders",
    "order_products__prior.csv":   "order_products_prior",
    "order_products__train.csv":   "order_products_train",
}

print(f"  {'CSV file':<35} {'CSV rows':>10} {'DB rows':>10} {'Match':>7}")
print(f"  {'-'*35} {'-'*10} {'-'*10} {'-'*7}")

for fname, table in CSV_TABLE_MAP.items():
    fpath = os.path.join(DATA, fname)

    # Count lines minus the header
    with open(fpath, "r", encoding="utf-8") as f:
        csv_rows = sum(1 for _ in f) - 1

    db_rows = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    match   = csv_rows == db_rows
    status  = PASS if match else FAIL

    print(f"  {fname:<35} {csv_rows:>10,} {db_rows:>10,} {'OK' if match else 'MISMATCH':>7}")
    result(f"{table} row count", status,
           "" if match else f"CSV={csv_rows:,}  DB={db_rows:,}")

# ══════════════════════════════════════════════════════════════════════════════
# Check 2 — NULL analysis on days_since_prior_order
# ══════════════════════════════════════════════════════════════════════════════
banner("Check 2 — NULL analysis: days_since_prior_order")

total_orders, = con.execute("SELECT COUNT(*) FROM orders").fetchone()
null_count,   = con.execute(
    "SELECT COUNT(*) FROM orders WHERE days_since_prior_order IS NULL"
).fetchone()
first_orders, = con.execute(
    "SELECT COUNT(*) FROM orders WHERE order_number = 1"
).fetchone()

null_pct = null_count / total_orders * 100

print(f"  Total orders              : {total_orders:>10,}")
print(f"  orders with order_number=1: {first_orders:>10,}  (first orders, NULLs expected)")
print(f"  NULL days_since_prior     : {null_count:>10,}  ({null_pct:.2f}%)")

if null_count == first_orders:
    result("NULLs match first-order count exactly", PASS)
elif null_count > first_orders:
    result("More NULLs than first orders", WARN,
           f"{null_count - first_orders:,} unexpected NULLs")
else:
    result("Fewer NULLs than first orders", WARN,
           f"{first_orders - null_count:,} first orders have a non-NULL value")

# Distribution of days_since_prior_order (non-NULL)
stats = con.execute("""
    SELECT
        MIN(days_since_prior_order)  AS min_val,
        MAX(days_since_prior_order)  AS max_val,
        ROUND(AVG(days_since_prior_order), 2) AS avg_val
    FROM orders
    WHERE days_since_prior_order IS NOT NULL
""").fetchone()
print(f"\n  Non-NULL days_since_prior: min={stats[0]}, max={stats[1]}, avg={stats[2]}")

# ══════════════════════════════════════════════════════════════════════════════
# Check 3 — Orphaned records
# ══════════════════════════════════════════════════════════════════════════════
banner("Check 3 — Orphaned record checks")

orphan_checks = [
    (
        "order_products_prior: product_id not in products",
        """SELECT COUNT(*) FROM order_products_prior op
           LEFT JOIN products p ON op.product_id = p.product_id
           WHERE p.product_id IS NULL"""
    ),
    (
        "order_products_prior: order_id not in orders",
        """SELECT COUNT(*) FROM order_products_prior op
           LEFT JOIN orders o ON op.order_id = o.order_id
           WHERE o.order_id IS NULL"""
    ),
    (
        "order_products_train: product_id not in products",
        """SELECT COUNT(*) FROM order_products_train op
           LEFT JOIN products p ON op.product_id = p.product_id
           WHERE p.product_id IS NULL"""
    ),
    (
        "order_products_train: order_id not in orders",
        """SELECT COUNT(*) FROM order_products_train op
           LEFT JOIN orders o ON op.order_id = o.order_id
           WHERE o.order_id IS NULL"""
    ),
    (
        "products: aisle_id not in aisles",
        """SELECT COUNT(*) FROM products p
           LEFT JOIN aisles a ON p.aisle_id = a.aisle_id
           WHERE a.aisle_id IS NULL"""
    ),
    (
        "products: department_id not in departments",
        """SELECT COUNT(*) FROM products p
           LEFT JOIN departments d ON p.department_id = d.department_id
           WHERE d.department_id IS NULL"""
    ),
    (
        "order_details: product_id not in products",
        """SELECT COUNT(*) FROM order_details od
           LEFT JOIN products p ON od.product_id = p.product_id
           WHERE p.product_id IS NULL"""
    ),
]

for label, sql in orphan_checks:
    count, = con.execute(sql).fetchone()
    status = PASS if count == 0 else FAIL
    result(label, status,
           f"{count:,} orphaned rows" if count else "")

# ══════════════════════════════════════════════════════════════════════════════
# Summary
# ══════════════════════════════════════════════════════════════════════════════
banner("Validation Summary")

if not issues:
    print("  All checks passed. Dataset is clean.\n")
else:
    print(f"  {len(issues)} issue(s) found:\n")
    for iss in issues:
        print(f"    {iss}")
    print()

con.close()

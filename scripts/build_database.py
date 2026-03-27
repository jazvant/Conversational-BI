"""
M1.1 - M1.4 | Instacart DuckDB build script
=============================================
Creates models/instacart.db with:
  * 6 base tables loaded from /data CSVs   (M1.1)
  * Primary-key / foreign-key constraints  (M1.2)
  * Unified order_details table            (M1.4)
  * Schema metadata exported to docs/      (M2 prep)

Run with:
    .venv/Scripts/python scripts/build_database.py
"""

import os
import sys
import time
import textwrap

import duckdb
from tqdm import tqdm

# -- Paths --------------------------------------------------------------------
ROOT   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA   = os.path.join(ROOT, "data")
MODELS = os.path.join(ROOT, "models")
DOCS   = os.path.join(ROOT, "docs")
DB     = os.path.join(MODELS, "instacart.db")

os.makedirs(MODELS, exist_ok=True)
os.makedirs(DOCS,   exist_ok=True)

# Remove stale DB so the build is always reproducible
if os.path.exists(DB):
    os.remove(DB)
    print(f"Removed existing {DB}")

con = duckdb.connect(DB)

# -- Helpers ------------------------------------------------------------------
def banner(msg):
    print(f"\n{'-'*60}\n{msg}\n{'-'*60}")

def csv_path(name):
    """Return forward-slash path DuckDB expects on Windows."""
    return os.path.join(DATA, name).replace("\\", "/")

def rowcount(table):
    return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


# =============================================================================
# M1.1 + M1.2 -- Ingestion with explicit schema (PKs + FKs inline)
# DuckDB 1.x supports FK constraints in CREATE TABLE DDL but NOT via
# ALTER TABLE ADD FOREIGN KEY, so we declare everything upfront.
# =============================================================================
banner("M1.1 + M1.2 -- Creating tables with PK/FK constraints and loading CSVs")

# Step 1: create all table schemas with constraints -------------------------

con.execute("""
    CREATE TABLE aisles (
        aisle_id  INTEGER PRIMARY KEY,
        aisle     VARCHAR NOT NULL
    )
""")

con.execute("""
    CREATE TABLE departments (
        department_id  INTEGER PRIMARY KEY,
        department     VARCHAR NOT NULL
    )
""")

con.execute("""
    CREATE TABLE products (
        product_id    INTEGER PRIMARY KEY,
        product_name  VARCHAR NOT NULL,
        aisle_id      INTEGER NOT NULL,
        department_id INTEGER NOT NULL,
        FOREIGN KEY (aisle_id)      REFERENCES aisles(aisle_id),
        FOREIGN KEY (department_id) REFERENCES departments(department_id)
    )
""")

con.execute("""
    CREATE TABLE orders (
        order_id                INTEGER PRIMARY KEY,
        user_id                 INTEGER NOT NULL,
        eval_set                VARCHAR NOT NULL,
        order_number            INTEGER NOT NULL,
        order_dow               INTEGER NOT NULL,
        order_hour_of_day       INTEGER NOT NULL,
        days_since_prior_order  DOUBLE          -- NULL for first orders
    )
""")

con.execute("""
    CREATE TABLE order_products_prior (
        order_id           INTEGER NOT NULL,
        product_id         INTEGER NOT NULL,
        add_to_cart_order  INTEGER NOT NULL,
        reordered          INTEGER NOT NULL,
        PRIMARY KEY (order_id, product_id),
        FOREIGN KEY (order_id)   REFERENCES orders(order_id),
        FOREIGN KEY (product_id) REFERENCES products(product_id)
    )
""")

con.execute("""
    CREATE TABLE order_products_train (
        order_id           INTEGER NOT NULL,
        product_id         INTEGER NOT NULL,
        add_to_cart_order  INTEGER NOT NULL,
        reordered          INTEGER NOT NULL,
        PRIMARY KEY (order_id, product_id),
        FOREIGN KEY (order_id)   REFERENCES orders(order_id),
        FOREIGN KEY (product_id) REFERENCES products(product_id)
    )
""")

print("  All table schemas created.")

# Step 2: load data -- lookup tables first (FK dependencies) ----------------
LOAD_PLAN = [
    # (table,                  csv_file,                      dependencies_ready)
    ("aisles",                 "aisles.csv"),
    ("departments",            "departments.csv"),
    ("products",               "products.csv"),
    ("orders",                 "orders.csv"),
    ("order_products_prior",   "order_products__prior.csv"),
    ("order_products_train",   "order_products__train.csv"),
]

for table, fname in tqdm(LOAD_PLAN, desc="Loading data", unit="table"):
    t0 = time.time()
    # read_csv_auto streams lazily; INSERT INTO materialises in one pass
    con.execute(f"""
        INSERT INTO {table}
        SELECT * FROM read_csv_auto('{csv_path(fname)}', header=true, all_varchar=false)
    """)
    rows = rowcount(table)
    print(f"  {table:<30} {rows:>12,} rows  ({time.time()-t0:.1f}s)")

# Confirm FK metadata is visible
print("\n  Foreign key relationships registered:")
fk_meta = [
    ("products",              "aisle_id",      "aisles",      "aisle_id"),
    ("products",              "department_id", "departments", "department_id"),
    ("order_products_prior",  "order_id",      "orders",      "order_id"),
    ("order_products_prior",  "product_id",    "products",    "product_id"),
    ("order_products_train",  "order_id",      "orders",      "order_id"),
    ("order_products_train",  "product_id",    "products",    "product_id"),
]
for child_t, child_col, parent_t, parent_col in fk_meta:
    print(f"    {child_t}.{child_col} -> {parent_t}.{parent_col}")


# =============================================================================
# M1.4 -- Unified order_details table
# =============================================================================
banner("M1.4 -- Building unified order_details table")

con.execute("""
    CREATE TABLE order_details AS
    SELECT
        op.order_id,
        op.product_id,
        op.add_to_cart_order,
        op.reordered,
        o.eval_set,
        o.user_id,
        o.order_number,
        o.order_dow,
        o.order_hour_of_day,
        o.days_since_prior_order
    FROM (
        SELECT order_id, product_id, add_to_cart_order, reordered
        FROM order_products_prior
        UNION ALL
        SELECT order_id, product_id, add_to_cart_order, reordered
        FROM order_products_train
    ) op
    JOIN orders o ON op.order_id = o.order_id
""")

od_rows = rowcount("order_details")
print(f"  order_details rows: {od_rows:,}")
print(f"  Breakdown by eval_set:")
for row in con.execute(
    "SELECT eval_set, COUNT(*) AS cnt FROM order_details GROUP BY eval_set ORDER BY eval_set"
).fetchall():
    print(f"    {row[0]:<10} {row[1]:>12,}")


# =============================================================================
# Schema metadata -> docs/schema_metadata.txt  (M2 prep)
# =============================================================================
banner("M2 Prep -- Exporting schema metadata")

META_FILE  = os.path.join(DOCS, "schema_metadata.txt")
all_tables = [r[0] for r in con.execute("PRAGMA show_tables").fetchall()]

with open(META_FILE, "w", encoding="utf-8") as fh:
    fh.write("INSTACART DUCKDB -- SCHEMA METADATA\n")
    fh.write(f"Generated : {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    fh.write(f"Database  : {DB}\n")
    fh.write("=" * 60 + "\n\n")

    fh.write("TABLES\n------\n")
    for t in all_tables:
        fh.write(f"  {t}\n")
    fh.write("\n")

    for t in all_tables:
        fh.write("=" * 60 + "\n")
        fh.write(f"TABLE: {t}  ({rowcount(t):,} rows)\n")
        fh.write("=" * 60 + "\n")

        cols = con.execute(f"DESCRIBE {t}").fetchall()
        fh.write(f"{'Column':<30} {'Type':<20} {'Null':<6} {'Key'}\n")
        fh.write("-" * 65 + "\n")
        for col in cols:
            col_name, col_type, null, key, default, extra = col
            fh.write(f"{col_name:<30} {col_type:<20} {null:<6} {key or ''}\n")
        fh.write("\n")

    fh.write("=" * 60 + "\n")
    fh.write("FOREIGN KEY RELATIONSHIPS\n")
    fh.write("=" * 60 + "\n")
    fk_doc = textwrap.dedent("""\
        products.aisle_id              -> aisles.aisle_id
        products.department_id         -> departments.department_id
        order_products_prior.order_id  -> orders.order_id
        order_products_prior.product_id -> products.product_id
        order_products_train.order_id  -> orders.order_id
        order_products_train.product_id -> products.product_id
        order_details.order_id         -> orders.order_id   (via UNION ALL + JOIN)
        order_details.product_id       -> products.product_id (via UNION ALL + JOIN)
    """)
    fh.write(fk_doc)

    fh.write("\n" + "=" * 60 + "\n")
    fh.write("JOIN PATHS FOR LLM INTROSPECTION\n")
    fh.write("=" * 60 + "\n")
    join_doc = textwrap.dedent("""\
        Full product enrichment (name, aisle, department):
          order_details
            JOIN products    ON order_details.product_id = products.product_id
            JOIN aisles      ON products.aisle_id        = aisles.aisle_id
            JOIN departments ON products.department_id   = departments.department_id

        User order history:
          order_details
            JOIN orders ON order_details.order_id = orders.order_id

        Segment by eval_set (prior / train / test):
          SELECT * FROM order_details WHERE eval_set = 'prior'  -- 32M rows
          SELECT * FROM order_details WHERE eval_set = 'train'  -- 1.38M rows
    """)
    fh.write(join_doc)

print(f"  Saved -> {META_FILE}")


# -- Final summary ------------------------------------------------------------
banner("Build complete")
print(f"  Database : {DB}")
print(f"  Tables   :")
for t in all_tables:
    print(f"    {t:<35} {rowcount(t):>12,} rows")

db_size_mb = os.path.getsize(DB) / 1_048_576
print(f"\n  DB size  : {db_size_mb:.1f} MB")

con.close()
print("\nDone.")

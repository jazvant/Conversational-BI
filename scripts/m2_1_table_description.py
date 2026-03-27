"""
M2.1 — Table Description
=========================
Parses structural metadata from docs/schema_metadata.txt (no DB queries).
Queries instacart.db live for sample values only.
Exports describe_all_tables() → schema_raw dict for downstream modules.
"""

import logging
import os
import re
import sys

import duckdb

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# -- Constants ----------------------------------------------------------------
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DB_PATH       = os.path.join(_ROOT, "models", "instacart.db")
METADATA_PATH = os.path.join(_ROOT, "docs",   "schema_metadata.txt")

TABLES = [
    "orders",
    "order_products_prior",
    "order_products_train",
    "order_details",
    "products",
    "aisles",
    "departments",
]

# Columns whose full distinct-value set matters for LLM understanding
_FLAG_COLUMNS = {"reordered", "eval_set", "order_dow"}


# -- Parsing ------------------------------------------------------------------
def parse_metadata_file(path: str) -> dict:
    """
    Parse docs/schema_metadata.txt and return a dict keyed by table name.

    Each value:
        {
            "row_count": int,
            "columns":   list[dict]   # keys: name, type, nullable, is_pk
        }

    File-parsing only — no database connections.
    """
    with open(path, encoding="utf-8") as fh:
        text = fh.read()

    # Match each TABLE block: TABLE: <name>  (<count> rows)
    # followed by the column lines (between the dashes and the next ===)
    table_pattern = re.compile(
        r"^TABLE:\s+(\S+)\s+\(([0-9,]+)\s+rows\)\s*$",
        re.MULTILINE,
    )
    col_line_pattern = re.compile(
        r"^(\S+)\s{2,}(\S+)\s{2,}(YES|NO)\s*(PRI)?\s*$",
        re.MULTILINE,
    )

    result: dict = {}

    # Split on section dividers to get blocks
    blocks = re.split(r"^={4,}$", text, flags=re.MULTILINE)

    for i, block in enumerate(blocks):
        m = table_pattern.search(block)
        if not m:
            continue

        table_name = m.group(1)
        row_count  = int(m.group(2).replace(",", ""))

        # Column definitions are in the *next* block (after the === divider)
        col_block = blocks[i + 1] if i + 1 < len(blocks) else ""

        columns = []
        for cm in col_line_pattern.finditer(col_block):
            columns.append({
                "name":     cm.group(1),
                "type":     cm.group(2),
                "nullable": cm.group(3) == "YES",
                "is_pk":    cm.group(4) == "PRI",
            })

        result[table_name] = {
            "row_count": row_count,
            "columns":   columns,
        }

    return result


# -- Sample fetching ----------------------------------------------------------
def fetch_sample_values(con, table: str, columns: list) -> dict:
    """
    For each column in `columns` fetch a small number of distinct non-NULL
    sample values from `table`.

    Flag/categorical columns (reordered, eval_set, order_dow) get up to 5
    distinct values so the LLM sees the full range.  All others get 3.

    Returns: {col_name: [sample, ...]}
    """
    samples: dict = {}
    for col_info in columns:
        col = col_info["name"]
        limit = 5 if col in _FLAG_COLUMNS else 3
        try:
            rows = con.execute(
                f"SELECT DISTINCT {col} FROM {table} "
                f"WHERE {col} IS NOT NULL "
                f"ORDER BY {col} "
                f"LIMIT {limit}"
            ).fetchall()
            samples[col] = [r[0] for r in rows]
        except Exception as exc:
            log.warning("Could not sample %s.%s: %s", table, col, exc)
            samples[col] = []
    return samples


# -- Main entrypoint ----------------------------------------------------------
def describe_all_tables(con, tables: list, metadata_path: str) -> dict:
    """
    Merge file-based structural metadata with live sample values.

    Returns a dict keyed by table name:
        {
            "table":     str,
            "row_count": int,
            "columns":   list[dict],
            "samples":   dict[str, list]
        }
    """
    meta = parse_metadata_file(metadata_path)
    schema_raw: dict = {}

    for table in tables:
        if table not in meta:
            log.warning("Table %r not found in metadata file — skipping.", table)
            continue

        table_meta = meta[table]
        samples    = fetch_sample_values(con, table, table_meta["columns"])

        schema_raw[table] = {
            "table":     table,
            "row_count": table_meta["row_count"],
            "columns":   table_meta["columns"],
            "samples":   samples,
        }
        log.info(
            "Described %s  (%d cols, %d samples fetched)",
            table,
            len(table_meta["columns"]),
            len(samples),
        )

    return schema_raw


# -- CLI self-test ------------------------------------------------------------
if __name__ == "__main__":
    print("=== M2.1 self-test ===\n")

    # Structural parsing — no DB needed
    meta = parse_metadata_file(METADATA_PATH)
    assert "orders" in meta,                              "orders missing"
    assert meta["orders"]["row_count"] == 3_421_083,      "orders row count wrong"
    assert len(meta["orders"]["columns"]) == 7,           "orders column count wrong"
    assert meta["order_details"]["row_count"] == 33_819_106, "order_details row count wrong"
    print("parse_metadata_file assertions passed.")

    # Live sample queries
    con = duckdb.connect(DB_PATH, read_only=True)
    schema_raw = describe_all_tables(con, TABLES, METADATA_PATH)

    assert "days_since_prior_order" in schema_raw["orders"]["samples"]
    assert set(schema_raw["orders"]["samples"]["eval_set"]) == {"prior", "train", "test"}
    assert schema_raw["products"]["samples"]["aisle_id"] != []
    print("describe_all_tables assertions passed.")

    # Pretty-print a summary
    print("\nTable summary:")
    for t, info in schema_raw.items():
        print(f"  {t:<30} {info['row_count']:>12,} rows  "
              f"{len(info['columns'])} cols")

    con.close()
    print("\nM2.1 OK")

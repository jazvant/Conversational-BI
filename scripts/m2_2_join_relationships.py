"""
M2.2 — Join Relationships
==========================
Defines the FK graph for the Instacart DuckDB schema.
Validates referential integrity against live data.
Produces human-readable join path strings for prompt injection.
"""

import logging
import os
import sys

import duckdb

from m2_1_table_description import DB_PATH, TABLES
from config import FK_RELATIONSHIPS, TABLE_SIZE_HINTS

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# -- Tables to skip during live orphan checks --------------------------------
# order_details is a CTAS materialisation of the UNION ALL; its integrity is
# guaranteed by the source tables, so we skip it to avoid redundant 33M scans.
_SKIP_INTEGRITY_CHECK = {"order_details"}


# -- Functions ----------------------------------------------------------------
def describe_joins(fk_map: dict) -> list:
    """
    Return one string per FK in the format:
        "child_table.child_col -> parent_table.parent_col"
    """
    lines = []
    for child_table, fks in fk_map.items():
        for child_col, (parent_table, parent_col) in fks.items():
            lines.append(
                f"{child_table}.{child_col} -> {parent_table}.{parent_col}"
            )
    return lines


def check_referential_integrity(con) -> bool:
    """
    Run LEFT JOIN orphan checks for every FK in FK_RELATIONSHIPS,
    skipping tables in _SKIP_INTEGRITY_CHECK.

    Logs INFO for each passing check and WARNING on any failure.
    Returns True only if all checked FKs are clean.
    """
    all_clean = True

    for child_table, fks in FK_RELATIONSHIPS.items():
        if child_table in _SKIP_INTEGRITY_CHECK:
            log.info(
                "Skipping %s — integrity guaranteed by source tables.",
                child_table,
            )
            continue

        for child_col, (parent_table, parent_col) in fks.items():
            sql = f"""
                SELECT COUNT(*) FROM {child_table} c
                LEFT JOIN {parent_table} p ON c.{child_col} = p.{parent_col}
                WHERE p.{parent_col} IS NULL
            """
            orphans = con.execute(sql).fetchone()[0]
            label   = f"{child_table}.{child_col} -> {parent_table}.{parent_col}"

            if orphans == 0:
                log.info("[OK] %s — 0 orphans", label)
            else:
                log.warning("[FAIL] %s — %d orphaned rows", label, orphans)
                all_clean = False

    return all_clean


# -- CLI self-test ------------------------------------------------------------
if __name__ == "__main__":
    print("=== M2.2 self-test ===\n")

    joins = describe_joins(FK_RELATIONSHIPS)
    assert len(joins) == 8,                                          f"Expected 8 FKs, got {len(joins)}"
    assert "order_products_prior.order_id -> orders.order_id" in joins
    assert "products.aisle_id -> aisles.aisle_id" in joins
    print("describe_joins assertions passed.")
    print("\nAll join paths:")
    for j in joins:
        print(f"  {j}")

    con = duckdb.connect(DB_PATH, read_only=True)
    ok  = check_referential_integrity(con)
    assert ok, "Referential integrity check failed — orphaned rows detected"
    print(f"\ncheck_referential_integrity: {'PASS' if ok else 'FAIL'}")
    con.close()

    print("\nM2.2 OK")

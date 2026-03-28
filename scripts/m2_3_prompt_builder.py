"""
M2.3 — Prompt Builder
======================
Assembles the final schema_context string for LLM injection.
Combines parsed structure + sample values + join paths + query rules
into a single self-contained markdown string.

Sample values live here only — they are not in schema_metadata.txt.
"""

import os
from typing import Any

from m2_2_join_relationships import FK_RELATIONSHIPS, TABLE_SIZE_HINTS
from config import SEMANTIC_NOTES as _SEMANTIC_NOTES

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# -- Section builders ---------------------------------------------------------

def _section(title: str, body: str) -> str:
    bar = "=" * 60
    return f"\n{bar}\n## {title}\n{bar}\n{body}\n"


def _format_samples(col: str, values: list) -> str:
    if not values:
        return "no non-null samples"
    formatted = ", ".join(repr(v) for v in values)
    return formatted


def _build_head() -> str:
    return (
        "# Instacart Market Basket Analysis — Schema Context\n"
        "# Version: M2 | Use this context to answer analytical questions\n"
        "# about customer purchasing behaviour on the Instacart platform.\n"
        "#\n"
        "# IMPORTANT RULES\n"
        "#   1. Always query `order_details` as the primary fact table.\n"
        "#   2. Join to `products`, `aisles`, `departments` for product metadata.\n"
        "#   3. Never SELECT * from LARGE/MEDIUM tables; always project columns.\n"
        "#   4. Filter by eval_set when the question is specific to train/prior.\n"
        "#   5. days_since_prior_order IS NULL for a user's very first order.\n"
        "#   6. All DuckDB SQL — standard ANSI with window functions supported.\n"
    )


def _build_table_catalog(schema_raw: dict, size_hints: dict) -> str:
    lines = []
    for table, info in schema_raw.items():
        hint = size_hints.get(table, "")
        lines.append(f"\n### {table}")
        lines.append(f"Rows: {info['row_count']:,}   [{hint}]")
        lines.append("")
        lines.append(f"{'Column':<30} {'Type':<12} {'Null':<6} {'PK':<5} Samples / Notes")
        lines.append("-" * 80)

        samples = info.get("samples", {})
        for col in info["columns"]:
            name     = col["name"]
            typ      = col["type"]
            nullable = "YES" if col["nullable"] else "NO"
            pk_mark  = "PK" if col["is_pk"] else ""

            raw_samples = samples.get(name, [])
            sample_str  = _format_samples(name, raw_samples)

            # Append semantic note where applicable
            note = _SEMANTIC_NOTES.get(name, "")
            detail = f"{sample_str}  [{note}]" if note else sample_str

            lines.append(
                f"  {name:<28} {typ:<12} {nullable:<6} {pk_mark:<5} {detail}"
            )
        lines.append("")

    return "\n".join(lines)


def _build_join_graph(join_descriptions: list) -> str:
    lines = ["Foreign-key relationships (child → parent):\n"]
    for j in join_descriptions:
        lines.append(f"  {j}")

    lines += [
        "",
        "Common join patterns:",
        "",
        "  -- Product enrichment (name, aisle, department)",
        "  SELECT od.*, p.product_name, a.aisle, d.department",
        "  FROM order_details od",
        "  JOIN products    p ON od.product_id    = p.product_id",
        "  JOIN aisles      a ON p.aisle_id        = a.aisle_id",
        "  JOIN departments d ON p.department_id   = d.department_id",
        "",
        "  -- Segment by evaluation set",
        "  SELECT * FROM order_details WHERE eval_set = 'prior'   -- 32.4M rows",
        "  SELECT * FROM order_details WHERE eval_set = 'train'   -- 1.38M rows",
        "",
        "  -- User purchase history",
        "  SELECT user_id, COUNT(DISTINCT order_id) AS total_orders",
        "  FROM order_details",
        "  GROUP BY user_id",
        "  ORDER BY total_orders DESC",
        "  LIMIT 10;",
    ]
    return "\n".join(lines)


def _build_size_hints(size_hints: dict) -> str:
    lines = ["Table size tiers and performance guidance:\n"]
    for table, hint in size_hints.items():
        lines.append(f"  {table:<30} {hint}")
    lines += [
        "",
        "Performance rules:",
        "  - Always filter before aggregating on LARGE tables.",
        "  - Use LIMIT during exploration.",
        "  - Prefer COUNT(*) over COUNT(col) unless checking nullability.",
        "  - Use window functions (RANK, ROW_NUMBER) instead of self-joins.",
        "  - DuckDB parallelises automatically — no hints needed.",
    ]
    return "\n".join(lines)


def _build_query_rules() -> str:
    return "\n".join([
        "Analytical query guidelines for this dataset:",
        "",
        "  REORDER RATE",
        "    SELECT AVG(reordered) AS reorder_rate",
        "    FROM order_details;",
        "",
        "  TOP PRODUCTS BY ORDER FREQUENCY",
        "    SELECT p.product_name, COUNT(*) AS appearances",
        "    FROM order_details od",
        "    JOIN products p ON od.product_id = p.product_id",
        "    GROUP BY p.product_name",
        "    ORDER BY appearances DESC",
        "    LIMIT 20;",
        "",
        "  PEAK SHOPPING HOURS",
        "    SELECT order_hour_of_day, COUNT(*) AS orders",
        "    FROM order_details",
        "    GROUP BY order_hour_of_day",
        "    ORDER BY order_hour_of_day;",
        "",
        "  BASKET SIZE DISTRIBUTION",
        "    SELECT order_id, COUNT(*) AS items_in_basket",
        "    FROM order_details",
        "    GROUP BY order_id",
        "    ORDER BY items_in_basket DESC",
        "    LIMIT 10;",
        "",
        "  NULL AWARENESS",
        "    -- days_since_prior_order is NULL for order_number = 1",
        "    -- Always use IS NOT NULL when computing averages:",
        "    SELECT AVG(days_since_prior_order)",
        "    FROM order_details",
        "    WHERE days_since_prior_order IS NOT NULL;",
    ])


# -- Main assembler -----------------------------------------------------------

def build_schema_prompt(
    schema_raw: dict,
    join_descriptions: list,
    size_hints: dict,
) -> str:
    """
    Assemble the full LLM schema context string.

    Sections (in order):
        1. Head         — dataset overview + immutable query rules
        2. Table Catalog — per-table column spec with live sample values
        3. Join Graph   — FK map + common join patterns
        4. Size Hints   — row-count tiers + performance guidance
        5. Query Rules  — annotated example queries

    Returns a single markdown string ready for LLM injection.
    """
    parts = [
        _build_head(),
        _section("Table Catalog",  _build_table_catalog(schema_raw, size_hints)),
        _section("Join Graph",     _build_join_graph(join_descriptions)),
        _section("Size Hints & Performance", _build_size_hints(size_hints)),
        _section("Example Query Patterns",   _build_query_rules()),
    ]
    return "\n".join(parts)


# -- CLI self-test ------------------------------------------------------------
if __name__ == "__main__":
    import duckdb
    from m2_1_table_description import DB_PATH, METADATA_PATH, TABLES, describe_all_tables
    from m2_2_join_relationships import describe_joins

    con        = duckdb.connect(DB_PATH, read_only=True)
    schema_raw = describe_all_tables(con, TABLES, METADATA_PATH)
    con.close()

    join_desc  = describe_joins(FK_RELATIONSHIPS)
    prompt     = build_schema_prompt(schema_raw, join_desc, TABLE_SIZE_HINTS)

    assert "## Table Catalog" in prompt
    assert "## Join Graph" in prompt
    assert "## Size Hints" in prompt
    assert "order_details" in prompt
    assert "days_since_prior_order" in prompt

    print(prompt[:2000])
    print(f"\n... (total {len(prompt):,} characters)")
    print("\nM2.3 OK")

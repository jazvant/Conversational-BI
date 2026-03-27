"""
M2 Entrypoint
=============
Orchestrates M2.1 -> M2.2 -> M2.3 and writes docs/schema_context.txt.

Run with:
    .venv/Scripts/python scripts/m2_run.py

Outputs:
    docs/schema_context.txt   -- LLM-ready schema prompt string
"""

import logging
import os
import sys
import time

import duckdb

# Ensure scripts/ is on the path when run from the project root
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from m2_1_table_description import (
    DB_PATH,
    METADATA_PATH,
    TABLES,
    describe_all_tables,
)
from m2_2_join_relationships import (
    FK_RELATIONSHIPS,
    TABLE_SIZE_HINTS,
    check_referential_integrity,
    describe_joins,
)
from m2_3_prompt_builder import build_schema_prompt

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

_ROOT   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR = os.path.join(_ROOT, "docs")
OUT_FILE = os.path.join(OUT_DIR, "schema_context.txt")


def main() -> None:
    t_total = time.time()

    # -- 1. Connect -----------------------------------------------------------
    log.info("Connecting to %s", DB_PATH)
    if not os.path.exists(DB_PATH):
        log.error("Database not found — run scripts/build_database.py first.")
        sys.exit(1)
    con = duckdb.connect(DB_PATH, read_only=True)

    # -- 2. M2.1 — describe all tables ----------------------------------------
    log.info("M2.1  Parsing metadata + fetching samples ...")
    t0 = time.time()
    schema_raw = describe_all_tables(con, TABLES, METADATA_PATH)
    log.info("M2.1  Done in %.1fs", time.time() - t0)

    # -- 3. M2.2 — join graph + integrity check --------------------------------
    log.info("M2.2  Building join graph ...")
    join_descriptions = describe_joins(FK_RELATIONSHIPS)

    log.info("M2.2  Checking referential integrity ...")
    t0 = time.time()
    integrity_ok = check_referential_integrity(con)
    log.info("M2.2  Integrity %s in %.1fs",
             "PASSED" if integrity_ok else "FAILED", time.time() - t0)
    if not integrity_ok:
        log.warning("Orphaned rows detected — schema context may be unreliable.")

    # -- 4. M2.3 — build prompt -----------------------------------------------
    log.info("M2.3  Assembling schema context prompt ...")
    t0 = time.time()
    prompt = build_schema_prompt(schema_raw, join_descriptions, TABLE_SIZE_HINTS)
    log.info("M2.3  Built %d characters in %.1fs", len(prompt), time.time() - t0)

    # -- 5. Write output -------------------------------------------------------
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(OUT_FILE, "w", encoding="utf-8") as fh:
        fh.write(prompt)
    log.info("Written -> %s", OUT_FILE)

    con.close()

    # -- 6. Summary ------------------------------------------------------------
    elapsed = time.time() - t_total
    print(f"\n{'='*60}")
    print("M2 Schema Introspection Module — complete")
    print(f"{'='*60}")
    print(f"  Tables described  : {len(schema_raw)}")
    print(f"  FK relationships  : {len(join_descriptions)}")
    print(f"  Integrity check   : {'PASS' if integrity_ok else 'FAIL'}")
    print(f"  Prompt length     : {len(prompt):,} characters")
    print(f"  Output file       : {OUT_FILE}")
    print(f"  Total time        : {elapsed:.1f}s")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()

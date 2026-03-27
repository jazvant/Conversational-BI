"""
Environment and data integrity verification script.
Run with: .venv/Scripts/python scripts/verify_setup.py
"""

import os
import sys

# ── 1. Library imports ────────────────────────────────────────────────────────
print("Checking library imports...")
try:
    import duckdb
    import pandas as pd
    import pyarrow as pa
    from tqdm import tqdm
    print(f"  duckdb    {duckdb.__version__}")
    print(f"  pandas    {pd.__version__}")
    print(f"  pyarrow   {pa.__version__}")
    import tqdm as _tqdm
    print(f"  tqdm      {_tqdm.__version__}")
    print("  All imports OK\n")
except ImportError as e:
    print(f"  FAILED: {e}")
    sys.exit(1)

# ── 2. Data directory scan ────────────────────────────────────────────────────
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")

print(f"Scanning: {DATA_DIR}\n")

csv_files = sorted(
    f for f in os.listdir(DATA_DIR) if f.endswith(".csv")
)

if not csv_files:
    print("  No CSV files found — check the /data directory.")
    sys.exit(1)

# ── 3. Print results table ────────────────────────────────────────────────────
col_w = [max(len("File"), max(len(f) for f in csv_files)),
         10,
         9]

header = (
    f"{'File':<{col_w[0]}}  {'Size':>{col_w[1]}}  {'Readable':<{col_w[2]}}"
)
divider = "-" * len(header)

print(header)
print(divider)

all_ok = True
for fname in tqdm(csv_files, desc="Verifying", unit="file"):
    fpath = os.path.join(DATA_DIR, fname)
    size_bytes = os.path.getsize(fpath)

    # Human-readable size
    if size_bytes >= 1_048_576:
        size_str = f"{size_bytes / 1_048_576:.1f} MB"
    elif size_bytes >= 1024:
        size_str = f"{size_bytes / 1024:.1f} KB"
    else:
        size_str = f"{size_bytes} B"

    # Readability check — read just the header row
    try:
        pd.read_csv(fpath, nrows=1)
        readable = "YES"
    except Exception as e:
        readable = f"FAIL ({e})"
        all_ok = False

    print(f"{fname:<{col_w[0]}}  {size_str:>{col_w[1]}}  {readable:<{col_w[2]}}")

print(divider)
print(f"\n{'All files readable.' if all_ok else 'WARNING: some files could not be read.'}")

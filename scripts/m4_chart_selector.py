"""
M4 Chart Selector
==================
Inspects a result DataFrame and returns the most appropriate chart type
for BI analysis, plus a list of all valid alternatives.
"""

import pandas as pd

# -- Chart type constants -----------------------------------------------------
TABLE     = "TABLE"
BAR       = "BAR"
LINE      = "LINE"
PIE       = "PIE"
SCATTER   = "SCATTER"
HISTOGRAM = "HISTOGRAM"
HEATMAP   = "HEATMAP"
AREA      = "AREA"

ALL_CHART_TYPES = [TABLE, BAR, LINE, PIE, SCATTER, HISTOGRAM, HEATMAP, AREA]

# Column-name keywords that indicate semantic roles
_TEMPORAL_KW = {"dow", "hour", "day", "week", "month", "number", "sequence"}
_METRIC_KW   = {"count", "cnt", "avg", "mean", "sum", "total", "rate",
                 "pct", "percent", "score", "ratio", "size"}


def _name_hits(col: str, keywords: set) -> bool:
    """Return True if any keyword appears as a substring of the lower-cased column name."""
    col_lower = col.lower()
    return any(kw in col_lower for kw in keywords)


# -- Column classification ----------------------------------------------------

def classify_columns(df: pd.DataFrame) -> dict:
    """
    Classify each DataFrame column into categorical, numeric, temporal, or boolean.

    Rules (applied in this priority order per column):
      1. Any integer column whose name contains a temporal keyword → temporal
      2. bool dtype → boolean
      3. object / string dtype → categorical
      4. float / double dtype → numeric
      5. Integer dtype:
           a. Name contains a metric keyword → numeric
           b. All non-null values are in {0, 1} → boolean
           c. nunique <= 31 AND max <= 31 → categorical (ordinal code)
           d. Otherwise → numeric
    """
    categorical: list = []
    numeric: list     = []
    temporal: list    = []
    boolean: list     = []

    for col in df.columns:
        series = df[col].dropna()
        dtype  = df[col].dtype

        # -- Integer columns: metric keyword wins over temporal ----------------
        if pd.api.types.is_integer_dtype(dtype):
            if _name_hits(col, _METRIC_KW):
                numeric.append(col)
            elif _name_hits(col, _TEMPORAL_KW):
                temporal.append(col)
            elif len(series) > 0 and set(series.unique()).issubset({0, 1}):
                boolean.append(col)
            elif series.nunique() <= 31 and (len(series) == 0 or series.max() <= 31):
                categorical.append(col)
            else:
                numeric.append(col)

        # -- Boolean dtype -----------------------------------------------------
        elif dtype == bool:
            boolean.append(col)

        # -- Object / string → categorical ------------------------------------
        elif dtype == object or str(dtype) in ("string", "StringDtype"):
            categorical.append(col)

        # -- Float / double → always numeric ----------------------------------
        elif pd.api.types.is_float_dtype(dtype):
            numeric.append(col)

        # -- Fallback ----------------------------------------------------------
        else:
            numeric.append(col)

    return {
        "categorical": categorical,
        "numeric":     numeric,
        "temporal":    temporal,
        "boolean":     boolean,
    }


# -- Chart type detection -----------------------------------------------------

def detect_chart_type(df: pd.DataFrame) -> tuple:
    """
    Return (recommended_chart, all_valid_alternatives) for a given DataFrame.

    Detection rules are applied in priority order (1 = highest priority).
    Special overrides are applied after the main rules.
    """
    if df.empty or len(df.columns) == 0:
        return TABLE, [TABLE]

    cc    = classify_columns(df)
    cats  = cc["categorical"]
    nums  = cc["numeric"]
    temps = cc["temporal"]

    n_cats  = len(cats)
    n_nums  = len(nums)
    n_temps = len(temps)
    n_rows  = len(df)

    # -- Main detection rules -------------------------------------------------

    # Rule 1: no plottable data at all
    if n_nums == 0 and n_temps == 0:
        recommended, alts = TABLE, [TABLE]

    # Rule 4: 1 temporal + 1 numeric → LINE  (checked before 2/3 to take priority)
    elif n_temps == 1 and n_nums == 1 and n_cats == 0:
        recommended, alts = LINE, [LINE, BAR, AREA, TABLE]

    # Rules 2 & 3: 1 categorical + 1 numeric
    elif n_cats == 1 and n_nums == 1 and n_temps == 0:
        cat_col = cats[0]
        num_col = nums[0]
        n_unique = df[cat_col].nunique()
        total    = df[num_col].sum()

        if 95 <= total <= 105:
            # Rule 3: percentages → PIE
            recommended, alts = PIE, [PIE, BAR, TABLE]
        elif n_unique <= 20:
            # Rule 2: typical categorical bar
            recommended, alts = BAR, [BAR, PIE, TABLE]
        else:
            # Too many categories for PIE; fallback to BAR without PIE option
            recommended, alts = BAR, [BAR, TABLE]

    # Rule 5: 2+ numeric columns, no categorical/temporal → SCATTER
    elif n_nums >= 2 and n_cats == 0 and n_temps == 0:
        recommended, alts = SCATTER, [SCATTER, LINE, TABLE]

    # Rule 6: 2 categorical + 1 numeric → HEATMAP
    elif n_cats == 2 and n_nums == 1 and n_temps == 0:
        recommended, alts = HEATMAP, [HEATMAP, BAR, TABLE]

    # Rule 7: 1 numeric column only → HISTOGRAM
    elif n_nums == 1 and n_cats == 0 and n_temps == 0:
        recommended, alts = HISTOGRAM, [HISTOGRAM, TABLE]

    # Rule 8: 1 categorical + 2+ numeric → grouped BAR
    elif n_cats == 1 and n_nums >= 2:
        recommended, alts = BAR, [BAR, LINE, TABLE]

    # Rule 9: fallback
    else:
        recommended, alts = TABLE, [TABLE]

    # -- Special overrides ----------------------------------------------------
    all_names = " ".join(df.columns.str.lower())
    if ("dow" in all_names or "hour" in all_names) and HEATMAP not in alts:
        alts.append(HEATMAP)

    if n_rows > 50 and recommended == PIE:
        recommended = BAR

    if n_rows == 1:
        recommended = TABLE

    return recommended, alts


# -- Self-test ----------------------------------------------------------------
if __name__ == "__main__":
    # Rule 2 — BAR
    df = pd.DataFrame({"department": ["produce", "dairy"], "count": [100, 80]})
    chart, alts = detect_chart_type(df)
    assert chart == BAR, f"Expected BAR, got {chart}"
    assert PIE in alts, f"PIE not in {alts}"
    print(f"Rule 2: {chart}, alts={alts}")

    # Rule 4 — LINE
    df = pd.DataFrame({"order_number": [1, 2, 3, 4, 5], "avg_days": [0, 7, 8, 9, 10]})
    chart, alts = detect_chart_type(df)
    assert chart == LINE, f"Expected LINE, got {chart}"
    print(f"Rule 4: {chart}, alts={alts}")

    # Rule 7 — HISTOGRAM
    df = pd.DataFrame({"days_since_prior_order": [1.0, 7.0, 14.0, 30.0]})
    chart, alts = detect_chart_type(df)
    assert chart == HISTOGRAM, f"Expected HISTOGRAM, got {chart}"
    print(f"Rule 7: {chart}, alts={alts}")

    # Fallback — TABLE (4 columns all classified as categorical, also 1 row override)
    df = pd.DataFrame({"a": [1], "b": [2], "c": [3], "d": [4]})
    chart, alts = detect_chart_type(df)
    assert chart == TABLE, f"Expected TABLE, got {chart}"
    print(f"Fallback: {chart}, alts={alts}")

    # DOW override — HEATMAP added to alts
    df = pd.DataFrame({"order_dow": [0, 1, 2], "total_orders": [1000, 2000, 1500]})
    chart, alts = detect_chart_type(df)
    assert HEATMAP in alts, f"HEATMAP should be in alts for dow column: {alts}"
    print(f"DOW override: {chart}, alts={alts}")

    print("\nM4 chart selector OK")

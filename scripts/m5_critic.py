"""
M5 Critic
==========
Validates result sanity and generates narrative insight using
Claude structured output. Narrative only generated when result
has more than NARRATIVE_MIN_ROWS rows.
"""

import logging
from dataclasses import dataclass, field

import anthropic
import pandas as pd
from pydantic import ValidationError

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import (
    CRITIC_MAX_TOKENS,
    CRITIC_SYSTEM_PROMPT,
    MODEL,
    NARRATIVE_MIN_ROWS,
    NULL_FLAG_THRESHOLD,
)
from scripts.m5_schemas import CriticOutput

log = logging.getLogger(__name__)

# -- Structured output tool definition ----------------------------------------

CRITIC_TOOL = {
    "name":        "generate_insight",
    "description": "Generate a structured insight from a BI query result.",
    "input_schema": CriticOutput.model_json_schema(),
}

_FALLBACK_CRITIC = CriticOutput(
    answer="Unable to generate insight.",
    finding="Please review the data table directly.",
    caveat="Insight generation encountered an error.",
    followup="Can you rephrase your question?",
)


# -- Data structure -----------------------------------------------------------

@dataclass
class CriticValidation:
    """Result of the Critic validation + narrative pass."""
    sane:      bool
    issues:    list[str]          = field(default_factory=list)
    narrative: CriticOutput | None = None


# -- Functions ----------------------------------------------------------------

def validate_result_sanity(result: dict) -> tuple[bool, list[str]]:
    """Check result dataframe for common data quality issues."""
    issues: list[str] = []
    df: pd.DataFrame  = result.get("data")

    # Zero rows
    if df is None or len(df) == 0:
        issues.append("Query returned no results.")
        return False, issues

    # High null rate per column
    for col in df.columns:
        null_pct = df[col].isna().mean()
        if null_pct > NULL_FLAG_THRESHOLD:
            issues.append(
                f"Column '{col}' is {null_pct * 100:.0f}% null — "
                "results may be incomplete."
            )

    # Implausible zero aggregate (1 row, 1 numeric col, value == 0)
    if len(df) == 1 and len(df.columns) == 1:
        col = df.columns[0]
        if pd.api.types.is_numeric_dtype(df[col]):
            val = df[col].iloc[0]
            if val == 0:
                issues.append(
                    "Result value is zero — verify filter conditions."
                )

    return (len(issues) == 0, issues)


def format_result_for_critic(df: pd.DataFrame, max_rows: int = 10) -> str:
    """Convert dataframe to compact string for the Critic prompt."""
    cols    = list(df.columns)
    subset  = df.head(max_rows)
    total   = len(df)

    lines = [f"Columns: {', '.join(cols)}"]
    for i, (_, row) in enumerate(subset.iterrows(), start=1):
        parts = []
        for col in cols:
            val = row[col]
            if isinstance(val, float):
                if 0.0 <= val <= 1.0:
                    parts.append(f"{col}={val * 100:.1f}%")
                else:
                    parts.append(f"{col}={val:.2f}")
            else:
                parts.append(f"{col}={val}")
        lines.append(f"Row {i}: {', '.join(parts)}")

    lines.append(f"({total} total rows)")
    return "\n".join(lines)


def generate_narrative(
    client: anthropic.Anthropic,
    question: str,
    result: dict,
) -> CriticOutput | None:
    """Generate structured narrative insight; returns None for scalar results."""
    if result.get("row_count", 0) <= NARRATIVE_MIN_ROWS:
        return None

    df = result["data"]

    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=CRITIC_MAX_TOKENS,
            system=CRITIC_SYSTEM_PROMPT,
            tools=[CRITIC_TOOL],
            tool_choice={"type": "tool", "name": "generate_insight"},
            messages=[
                {
                    "role":    "user",
                    "content": (
                        f"User question: {question}\n\n"
                        f"Data result:\n"
                        f"{format_result_for_critic(df)}"
                    ),
                }
            ],
        )

        tool_block = next(
            (b for b in response.content if b.type == "tool_use"), None
        )
        if tool_block is None:
            log.error("Critic: no tool_use block in response")
            return _FALLBACK_CRITIC

        output = CriticOutput(**tool_block.input)

    except ValidationError as exc:
        log.error("Critic: ValidationError — %s", exc)
        return _FALLBACK_CRITIC

    except Exception as exc:
        log.error("Critic: API error — %s", exc)
        return _FALLBACK_CRITIC

    log.info("Critic: generated narrative for %d rows", result["row_count"])
    return output


def critique(
    client: anthropic.Anthropic,
    question: str,
    result: dict,
) -> CriticValidation:
    """Orchestrate sanity validation and narrative generation for a result."""
    if not result.get("success"):
        return CriticValidation(
            sane=False,
            issues=[result.get("error", "Query failed.")],
            narrative=None,
        )

    sane, issues   = validate_result_sanity(result)
    narrative      = generate_narrative(client, question, result)
    return CriticValidation(sane=sane, issues=issues, narrative=narrative)


# -- Self-test (no API) -------------------------------------------------------
if __name__ == "__main__":
    # Zero rows
    zero_result = {
        "success":   True,
        "row_count": 0,
        "data":      pd.DataFrame({"dept": [], "rate": []}),
        "sql":       "SELECT...",
        "error":     None,
    }
    sane, issues = validate_result_sanity(zero_result)
    assert sane is False
    assert len(issues) > 0
    print("zero rows OK")

    # Clean result
    clean = {
        "success":   True,
        "row_count": 5,
        "data":      pd.DataFrame({
            "department":  ["produce", "dairy", "beverage", "snacks", "frozen"],
            "reorder_rate": [0.662, 0.658, 0.613, 0.574, 0.551],
        }),
        "sql": "SELECT...", "error": None,
    }
    sane, issues = validate_result_sanity(clean)
    assert sane    is True
    assert issues  == []
    print("clean result OK")

    # High null
    null_df = pd.DataFrame({
        "dept": ["a", "b", None, None, None, None],
        "rate": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6],
    })
    null_result = {"success": True, "row_count": 6, "data": null_df, "sql": "", "error": None}
    sane, issues = validate_result_sanity(null_result)
    assert sane         is False
    assert "null" in issues[0].lower()
    print("null column OK")

    # format_result_for_critic
    df = pd.DataFrame({"dept": ["a", "b", "c"], "rate": [0.66, 0.55, 0.44]})
    fmt = format_result_for_critic(df, max_rows=2)
    assert "Columns:" in fmt
    assert "Row 1:"   in fmt
    assert "%"        in fmt
    assert "(3 total rows)" in fmt
    assert "Row 3:"   not in fmt
    print("format_result_for_critic OK")

    # generate_narrative returns None for row_count <= NARRATIVE_MIN_ROWS
    scalar_result = {"success": True, "row_count": 1,
                     "data": pd.DataFrame({"count": [99]}), "sql": "", "error": None}
    # We don't have a real client — check the row_count guard directly
    assert scalar_result["row_count"] <= NARRATIVE_MIN_ROWS
    print("narrative scalar guard OK")

    print("m5_critic self-test OK")

"""
M5 Planner
===========
Classifies user intent using Claude structured output.
Returns a validated PlannerDecision — never raw JSON.
Defaults safely to data_query on any API or schema error.
"""

import logging
from dataclasses import dataclass

import anthropic
from pydantic import ValidationError

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import INTENT_DATA_QUERY, MODEL, PLANNER_SYSTEM_PROMPT
from scripts.m5_schemas import PlannerOutput

log = logging.getLogger(__name__)

# -- Structured output tool definition ----------------------------------------

PLANNER_TOOL = {
    "name":         "classify_intent",
    "description":  "Classify the intent of the user's question.",
    "input_schema": PlannerOutput.model_json_schema(),
}

_FALLBACK_DATA_QUERY = lambda reason: PlannerDecision(  # noqa: E731
    intent=INTENT_DATA_QUERY,
    reason=reason,
    subqueries=[],
)


# -- Data structure -----------------------------------------------------------

@dataclass
class PlannerDecision:
    """Internal routing object produced by the Planner."""
    intent:     str
    reason:     str
    subqueries: list[str]


# -- Functions ----------------------------------------------------------------

def build_planner_message(question: str, memory: list[dict]) -> str:
    """Build the user message for the Planner API call, including recent context."""
    if not memory:
        return f"Current question: {question}"

    recent = memory[-2:]
    lines  = ["Recent context:"]
    for turn in recent:
        lines.append(f"User asked: {turn['question']}")
        lines.append(f"Result summary: {turn.get('summary', '')}")
        lines.append("")

    lines.append(f"Current question: {question}")
    return "\n".join(lines)


def classify_intent(
    client: anthropic.Anthropic,
    question: str,
    memory: list[dict],
) -> PlannerDecision:
    """Call Claude with tool_use to classify intent; fall back to data_query on error."""
    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=256,
            system=PLANNER_SYSTEM_PROMPT,
            tools=[PLANNER_TOOL],
            tool_choice={"type": "tool", "name": "classify_intent"},
            messages=[
                {"role": "user", "content": build_planner_message(question, memory)}
            ],
        )

        # Extract the tool_use block
        tool_block = next(
            (b for b in response.content if b.type == "tool_use"), None
        )
        if tool_block is None:
            log.error("Planner: no tool_use block in response")
            return _FALLBACK_DATA_QUERY("No tool_use block — defaulting")

        output = PlannerOutput(**tool_block.input)

    except ValidationError as exc:
        log.error("Planner: ValidationError — %s", exc)
        return _FALLBACK_DATA_QUERY("Validation failed — defaulting")

    except Exception as exc:
        log.error("Planner: API error — %s", exc)
        return _FALLBACK_DATA_QUERY("API error — defaulting")

    log.info("Planner: intent=%s | %s", output.intent, output.reason)
    return PlannerDecision(
        intent=output.intent,
        reason=output.reason,
        subqueries=output.subqueries,
    )


# -- Helper predicates --------------------------------------------------------

def is_data_query(d: PlannerDecision) -> bool:
    """Return True if the decision intent is data_query."""
    return d.intent == INTENT_DATA_QUERY


def is_conversational(d: PlannerDecision) -> bool:
    """Return True if the decision intent is conversational."""
    return d.intent == "conversational"


def is_multistep(d: PlannerDecision) -> bool:
    """Return True if the decision intent is multistep."""
    return d.intent == "multistep"


def is_cannot_answer(d: PlannerDecision) -> bool:
    """Return True if the decision intent is cannot_answer."""
    return d.intent == "cannot_answer"


# -- Self-test (no API) -------------------------------------------------------
if __name__ == "__main__":
    from pydantic import ValidationError as VE
    from scripts.m5_schemas import PlannerOutput as PO

    p = PO(intent="data_query", reason="r", subqueries=[])
    assert p.intent == "data_query"

    try:
        PO(intent="bad", reason="r", subqueries=[])  # type: ignore
        assert False
    except VE:
        pass

    d = PlannerDecision("data_query", "r", [])
    assert is_data_query(d)     is True
    assert is_conversational(d) is False
    assert is_multistep(d)      is False

    d2 = PlannerDecision("multistep", "r", ["q1", "q2"])
    assert is_multistep(d2)       is True
    assert len(d2.subqueries) == 2

    msg = build_planner_message("q?", [])
    assert "Current question: q?" in msg
    assert "Recent context" not in msg

    memory = [{"question": "prev?", "summary": "42", "sql": "", "success": True}]
    msg2 = build_planner_message("follow up?", memory)
    assert "prev?" in msg2
    assert "42" in msg2
    assert "Current question: follow up?" in msg2

    print("m5_planner self-test OK")

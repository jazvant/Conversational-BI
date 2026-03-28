# Conversational BI Agent — Comprehensive Technical Report

---

## 1. Project Overview

This is a **conversational Business Intelligence (BI) agent** built on the Instacart Market Basket Analysis dataset. Users ask natural-language questions; the system translates them to SQL, executes against DuckDB, and returns data, charts, and AI-generated insights.

The project supports **two architectures** controlled by a single constant in `config.py`:

```
Architecture 1 (ARCHITECTURE=1):  Direct SQL generation only
Architecture 2 (ARCHITECTURE=2):  Planner + Critic agent layer
```

---

## 2. Top-Level Directory Structure

```
Conversational BI/
├── app.py                    ← Streamlit UI entry point
├── config.py                 ← Single source of truth for all config
├── requirements.txt          ← Python dependencies
├── models/instacart.db       ← DuckDB database
├── docs/
│   ├── schema_context.txt    ← Generated schema prompt
│   ├── schema_metadata.txt   ← Table metadata
│   └── benchmark_*.json      ← Benchmark results
├── scripts/
│   ├── build_database.py     ← One-time DB setup
│   ├── m2_*.py               ← Schema context generation
│   ├── m3_*.py               ← SQL pipeline
│   ├── m4_*.py               ← Chart selection & rendering
│   ├── m5_*.py               ← Architecture 2 Planner + Critic
│   ├── m6_*.py               ← Memory & summarisation
│   ├── m7_input_validator.py ← Security: SQL input gating
│   ├── m8_output_sanitiser.py← Security: output sanitisation
│   └── m10_benchmark.py      ← Evaluation suite
└── tests/                    ← 116 unit tests
```

---

## 3. System Architecture Block Diagram

### Architecture 2 (Full Pipeline)

```
┌─────────────────────────────────────────────────────────────────────┐
│                          USER (Streamlit)                           │
│                   Natural Language Question                         │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       M5 PLANNER                                    │
│           classify_intent()  →  PlannerDecision                     │
│                                                                     │
│   Claude API (forced tool_use)  →  PlannerOutput (Pydantic)         │
│                                                                     │
│   intent = data_query | conversational | multistep | cannot_answer  │
└────┬──────────────┬──────────────────┬──────────────────────────────┘
     │              │                  │
     ▼              ▼                  ▼
CANNOT_ANSWER  CONVERSATIONAL     DATA QUERY / MULTISTEP
     │              │                  │
     │              ▼                  ▼
     │     M5 CONVERSATIONAL      M3 SQL PIPELINE
     │     answer_from_memory()   ┌────────────────────┐
     │     (reads memory,         │ M3.1 Prompt Builder │
     │      no SQL, no DB)        │ M3.2 SQL Generator  │
     │                            │ M3.3 Executor       │
     │                            │ M3.4 Error Recovery │
     │                            └─────────┬──────────┘
     │                                      │
     │                                      ▼
     │                              M5 CRITIC
     │                              validate_result_sanity()
     │                              generate_narrative()
     │                              → CriticValidation
     │                                      │
     ▼                                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        RESULT DISPLAY                               │
│   Left col: chat history     Right col: chart + data + insight      │
│   M4 Chart Selector → M4 Renderer → Plotly figure                  │
└─────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       M6 MEMORY                                     │
│   add_turn() → {question, sql, summary, success}                    │
│   M6 Summariser: programmatic summarisation of result DataFrames    │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 4. Module-by-Module Breakdown

---

### 4.1 `config.py` — Central Configuration

**Purpose:** Single source of truth. Swapping datasets means swapping this file.

**Code blocks:**

| Block | Purpose |
|---|---|
| Paths | `DB_PATH`, `SCHEMA_CONTEXT_PATH`, `METADATA_PATH` |
| Model settings | `MODEL = "claude-sonnet-4-6"`, `MAX_TOKENS = 600` |
| Table registry | `TABLES` list, `FK_RELATIONSHIPS` dict |
| Size hints | `TABLE_SIZE_HINTS` injected into schema prompt |
| Semantic notes | `SEMANTIC_NOTES` — encoding docs (e.g. `order_dow: 0=Saturday`) |
| Query rules | `QUERY_RULES` — injected into system prompt as explicit constraints |
| Memory settings | `MAX_HISTORY_TURNS=3`, `MAX_HISTORY_TURNS_EXT=5`, `SUMMARY_ROW_LIMIT=10` |
| Multistep keywords | `MULTISTEP_KEYWORDS` — triggers extended memory window |
| Security | `DB_READ_ONLY`, `DUCKDB_MEMORY_LIMIT`, `QUERY_TIMEOUT_SECONDS`, `BLOCKED_KEYWORDS`, `NULL_FLAG_THRESHOLD`, `SAFE_ERROR_SUBSTRINGS` |
| Architecture 2 | `ARCHITECTURE=2`, intent constants, `CRITIC_MAX_TOKENS`, `NARRATIVE_MIN_ROWS`, system prompts |

**Modular:** Yes. All parameters are name-spaced and clearly commented. Changing datasets requires only editing this file.

**What it addresses:** Prevents magic numbers being scattered across 20+ files. Enables feature flagging (e.g. `ARCHITECTURE = 1` vs `2`).

---

### 4.2 `scripts/m3_1_prompt_builder.py` — Prompt Builder

**Purpose:** Assembles the Claude API payload for SQL generation.

**Code blocks:**

```
build_system_prompt(schema_context)
    ← Appends _SYSTEM_INSTRUCTIONS to the schema text

load_schema_context(path)
    ← Reads docs/schema_context.txt from disk

build_user_message(question, history)
    ← Constructs the messages[] array for the API call
    ← Prepends up to 6 prior history turns
```

**Modular:** Yes — pure functions, no I/O side effects beyond file read.

**Not modular:** `_SYSTEM_INSTRUCTIONS` is hardcoded inline, not in `config.py`. This means changing prompt instructions requires editing this file. A config-driven approach would be more flexible.

**Addresses:** Separates prompt construction from SQL generation and execution, following single-responsibility principle.

---

### 4.3 `scripts/m3_2_sql_generator.py` — SQL Generator

**Purpose:** Makes the Claude API call and returns a clean SQL string.

**Code blocks:**

```
generate_sql(client, system_prompt, messages)
    ← One API call, returns stripped SQL string

strip_markdown_fences(text)
    ← Removes ```sql ... ``` wrapping from Claude responses

is_cannot_answer(sql)
    ← Returns True if Claude responded "CANNOT_ANSWER"
```

**Modular:** Yes. Has no awareness of execution, validation, or memory. Pure API-call wrapper.

**Not modular:** `MODEL` and `MAX_TOKENS` are duplicated here (also in `config.py`), creating two sources of truth. The `config.py` values should be the single source.

**Addresses:** Isolates the fragile "API call + response parsing" step so it can be tested independently and swapped for a different LLM.

---

### 4.4 `scripts/m3_3_executor.py` — SQL Executor

**Purpose:** Executes SQL against DuckDB with full safety wrapping.

**Code blocks and flow:**

```
execute_sql(con, sql)
│
├─ Step 1: validate(sql)           ← M7 input validator
│   └─ if blocked → sanitise_result(failure) and return early
│
├─ Step 2: inject_limit(sql)       ← Appends LIMIT 1000 if missing
│
├─ Step 3: threaded execution
│   ├─ Thread: con.execute(sql).fetchdf()
│   ├─ thread.join(timeout=QUERY_TIMEOUT_SECONDS)
│   └─ if thread.is_alive() → timeout error
│
└─ Step 4: sanitise_result(result) ← M8 output sanitiser
```

**Modular:** Yes — delegates to M7 and M8 cleanly. Threading timeout is self-contained.

**Addresses:**
- Row explosion protection (`inject_limit`)
- Security: rejected before DB touch (M7)
- Timeout: daemon threads prevent process hanging
- Memory cap: `DUCKDB_MEMORY_LIMIT` set at connection time
- Error safety: M8 cleans all output

---

### 4.5 `scripts/m3_4_error_recovery.py` — Error Recovery & Retry Loop

**Purpose:** Orchestrates up to 3 attempts; on failure, feeds the error back to Claude.

**Code blocks:**

```
attempt_with_retry(client, con, system_prompt, question, history)
│
├─ Attempt 1: build_messages(question, history) → generate_sql → execute_sql
│
├─ Attempt 2 (on failure):
│   └─ build_retry_message(question, failed_sql, error) → generate_sql → execute_sql
│
└─ Attempt 3 (on failure): same retry pattern
    └─ Returns last result regardless of success
```

**Flow diagram:**

```
question
    │
    ▼
attempt=1  →  build_messages()  →  generate_sql()  →  execute_sql()
    │                                                        │
    │                                              SUCCESS? ─┤
    │                                               yes: return │
    ▼                                               no:  ───────┘
attempt=2  →  build_retry_message()  →  generate_sql()  →  execute_sql()
    │                                                        │
    ▼                                               SUCCESS? ─┤
attempt=3  →  build_retry_message()  →  generate_sql()  →  execute_sql()
    │                                                        │
    └─────────────────────────── return (success or final failure) ──┘
```

**Modular:** Yes. The retry loop doesn't know how SQL is generated or executed.

**Addresses:** Handles transient SQL errors (typos in table names, wrong column types) without user intervention.

---

### 4.6 `scripts/m4_chart_selector.py` — Chart Type Selector

**Purpose:** Inspects a result DataFrame and recommends the best chart type.

**Code blocks:**

```
classify_columns(df)
    ← Returns {categorical, numeric, temporal, boolean} column lists
    ← Priority rules: temporal keyword > metric keyword > value range

detect_chart_type(df)
    ← 9-rule priority decision tree
    ← Special overrides: dow/hour → add HEATMAP; >50 rows → demote PIE
```

**Decision tree (simplified):**

```
df.empty? → TABLE
no numerics? → TABLE
temporal + numeric → LINE
cat(1) + num(1):
    sum≈100%? → PIE
    ≤20 unique? → BAR
    else → BAR
2+ numerics → SCATTER
2 cats + 1 num → HEATMAP
1 numeric only → HISTOGRAM
cat(1) + 2+ num → grouped BAR
else → TABLE
```

**Modular:** Yes. Pure function, no side effects, unit-testable without API or DB.

**Not modular:** Column keyword lists (`_TEMPORAL_KW`, `_METRIC_KW`) are duplicated between this file and `m4_renderer.py`. They should be in a shared constants module.

---

### 4.7 `scripts/m4_renderer.py` — Chart Renderer

**Purpose:** Takes a DataFrame and chart-type string, returns a Plotly figure.

**Code blocks:**

| Function | Chart |
|---|---|
| `render_bar()` | Vertical or horizontal bar (auto-flips >10 categories) |
| `render_line()` | Line with markers, multi-series |
| `render_pie()` | Pie with percentage + label |
| `render_scatter()` | XY scatter with optional colour grouping |
| `render_histogram()` | 30-bin histogram |
| `render_heatmap()` | Pivot table → colour grid |
| `render_area()` | Filled area line |
| `render_table()` | Styled Plotly table (alternating dark rows) |
| `apply_theme(fig)` | Applies transparent background for Streamlit dark theme |
| `render_chart(df, type, title)` | Dispatcher to correct render function |

**Modular:** Yes. Each render function is independent. The `_RENDER_MAP` dispatcher makes adding a new chart type a 2-line change.

**Never calls Streamlit** — returns a `go.Figure`, letting the caller decide how to display it.

---

### 4.8 `scripts/m5_schemas.py` — Pydantic Schemas (Architecture 2)

**Purpose:** All structured data contracts for Architecture 2.

**Schemas:**

```
PlannerOutput (BaseModel)
├── intent: Literal["data_query","conversational","multistep","cannot_answer"]
├── reason: str
└── subqueries: list[str]     ← auto-cleared if intent != multistep

CriticOutput (BaseModel)
├── answer: str               ← non-empty enforced
├── finding: str              ← non-empty enforced
├── caveat: str               ← "None." if no issues
└── followup: str             ← auto-appends "?" if missing

BenchmarkQuestionResult (BaseModel, frozen=True)
├── id, category, question, sql
├── sql_correct, db_executed, result_sane
├── overall: Literal["PASS","PARTIAL","FAIL","BLOCKED_OK","BLOCKED_FAIL"]
└── latency_ms: int (≥0)

BenchmarkReport (BaseModel)
├── Architecture metadata, aggregate metrics
├── by_category: dict
└── questions: list[BenchmarkQuestionResult]
    ├── to_json() → model_dump_json()
    └── from_json() → model_validate_json()
```

**Modular:** Yes — all schemas in one place, imported everywhere else. Zero inline schema definitions in other files.

**Addresses:**
- Type safety: Pydantic v2 validates at instantiation time
- Immutability: `frozen=True` on benchmark records prevents mutation after scoring
- Self-healing: validators auto-fix minor issues (`followup` missing `?`, subqueries on non-multistep)
- Serialisation: `to_json`/`from_json` for benchmark persistence

---

### 4.9 `scripts/m5_planner.py` — Intent Classifier (Planner Agent)

**Purpose:** Classifies the user's question into one of four intents using Claude forced tool use.

**Code blocks:**

```
PLANNER_TOOL
    ← Anthropic tool definition with PlannerOutput.model_json_schema() as input_schema
    ← Forces Claude to fill the PlannerOutput fields

build_planner_message(question, memory)
    ← Includes last 2 memory turns as context
    ← "Recent context: ... Current question: ..."

classify_intent(client, question, memory) → PlannerDecision
    ← forced tool_use call (tool_choice={"type":"tool","name":"classify_intent"})
    ← Extracts tool_use block from response.content
    ← Validates via PlannerOutput(**tool_block.input)
    ← Falls back to data_query on ANY error (API, validation, missing block)

Helper predicates: is_data_query, is_conversational, is_multistep, is_cannot_answer
```

**Modular:** Yes. Returns a plain `PlannerDecision` dataclass, not a Pydantic model, so routing code doesn't need to understand Pydantic.

**Safe degradation:** Every error path returns `_FALLBACK_DATA_QUERY` — the system always tries to answer rather than failing completely.

---

### 4.10 `scripts/m5_critic.py` — Result Validator + Narrative Generator (Critic Agent)

**Purpose:** Validates result quality and generates a structured text insight.

**Code blocks:**

```
validate_result_sanity(result) → (bool, list[str])
    ├── Check 1: zero rows → False
    ├── Check 2: column null rate > 20% → flag per column
    └── Check 3: single-cell result == 0 → "verify filter conditions"

format_result_for_critic(df, max_rows=10) → str
    ← Converts DataFrame to compact text
    ← Floats 0-1 auto-formatted as percentages
    ← Shows "(N total rows)" footer

generate_narrative(client, question, result) → CriticOutput | None
    ← Returns None if row_count <= NARRATIVE_MIN_ROWS=2  (scalar results)
    ← Forced tool_use → CriticOutput fields
    ← Falls back to _FALLBACK_CRITIC on error

critique(client, question, result) → CriticValidation
    ← Master orchestrator: validate_result_sanity + generate_narrative
```

**Flow:**

```
result dict
    │
    ├── result["success"] == False?
    │       └── CriticValidation(sane=False, issues=[error], narrative=None)
    │
    └── success == True
            │
            ├── validate_result_sanity() → sane, issues
            │
            ├── row_count <= 2?
            │       └── generate_narrative returns None (no API call)
            │
            └── row_count > 2
                    └── Claude API (forced tool_use) → CriticOutput
                            └── CriticValidation(sane, issues, narrative)
```

**Modular:** Yes. Sanity validation is fully independent of narrative generation.

**Addresses:** Prevents wasting API tokens on trivial scalar results (`NARRATIVE_MIN_ROWS` guard). Surfaces data quality issues to the user.

---

### 4.11 `scripts/m5_conversational.py` — Conversational Answer Handler

**Purpose:** Handles questions classified as "conversational" — no SQL, no DB.

**Code blocks:**

```
build_conversational_context(question, memory) → str
    ├── Empty memory: returns "no prior history" message + prompt
    └── Non-empty: last 3 turns formatted as:
            "User asked: {q}\nResult: {summary}\n..."
            + "Current question: {q}"
            + instructions to use numbers from summaries

answer_from_memory(client, question, memory) → str
    ← Free-text Claude call (no structured output)
    ← 150-word limit
    ← Falls back to _FALLBACK string on error
```

**Modular:** Yes. Uses only the text summaries from memory — no DB access, no SQL awareness.

**What it addresses:** Enables "Why is produce ranked highest?" follow-ups without re-running a query. Uses the already-computed summaries in memory rather than re-fetching data.

---

### 4.12 `scripts/m6_memory.py` — Memory Manager

**Purpose:** Manages the per-session conversation memory with result summaries.

**Code blocks:**

```
add_turn(memory, question, sql, result) → list
    ← On success: calls summarise_result(result["data"], question, sql)
    ← On failure: "Query failed: {error_preview}"
    ← Appends {question, sql, summary, success} dict

build_messages(question, memory) → list[dict]
    ← Converts memory to Claude API messages format
    ← Dynamic window via get_window_size()
    ← Format: [{role:user, content:question}, {role:assistant, content:"sql\n[Result: summary]"}, ...]

get_window_size(question) → int
    ← Returns MAX_HISTORY_TURNS=3 normally
    ← Returns MAX_HISTORY_TURNS_EXT=5 if MULTISTEP_KEYWORDS detected

get_context_summary(memory) → str
    ← UI display helper for sidebar
```

**Memory entry structure:**

```python
{
    "question": "Top departments by reorder rate?",
    "sql":      "SELECT d.department, AVG(...)",
    "summary":  "Top 5 results for department by reorder_rate:\n 1. produce: 66.2%...",
    "success":  True
}
```

**Modular:** Yes. Clean interface — callers just call `add_turn` and `build_messages`. Internal summarisation details hidden.

---

### 4.13 `scripts/m6_summariser.py` — Programmatic Summariser

**Purpose:** Converts a result DataFrame to a compact text summary without any API calls.

**Code blocks and routing:**

```
summarise_result(df, question, sql) → str
    │
    ├── 0 rows → "Query returned no results."
    ├── 1 row × 1 col → summarise_scalar()    e.g. "Result: 3,421,083."
    ├── 1 row × N cols → summarise_single_row() e.g. "Single result — col: val, ..."
    ├── N rows × 2 cols → summarise_two_col()  e.g. "Top 10 results for dept by rate:\n 1. produce: 66.2%..."
    └── else → summarise_generic()              e.g. "25 rows × 4 columns returned..."
```

**Formatting intelligence:**

- Integers: comma-separated (`3,421,083`)
- Floats 0–1: percentage (`66.2%`)
- Large floats: comma integer (`1,234,567`)
- Other floats: 2 decimal places

**Modular:** Yes. Interface is `summarise_result(df, question, sql)` — the sub-summarisers are internal implementation detail.

**Not modular:** The shape-detection logic (1×1, 1×N, N×2) is tightly coupled to the routing function. Adding a new shape type requires modifying the routing tree.

**Addresses:** Zero-cost summaries that power the conversational memory. A LLM-based summariser could be swapped in by changing only this module.

---

### 4.14 `scripts/m7_input_validator.py` — Input Validator (Security Layer)

**Purpose:** SQL safety gating — validates before the query ever reaches DuckDB.

**Code blocks:**

```
validate(sql) → ValidationResult(allowed, reason)
    │
    ├── Check 1: empty SQL → blocked
    ├── Check 2: is_blocked_keyword() → first keyword in BLOCKED_KEYWORDS?
    │               BLOCKED_DDL: DROP CREATE ALTER TRUNCATE
    │               BLOCKED_DML: DELETE INSERT UPDATE MERGE REPLACE
    │               BLOCKED_FILE_OPS: COPY EXPORT IMPORT
    ├── Check 3: contains_inline_blocked_keyword() → word-boundary regex scan
    └── Check 4: is_suspicious_structure()
                    ├── Multiple statements (semicolon not at end)
                    ├── Block comment /* (obfuscation)
                    ├── Inline -- comment on non-first line
                    └── >3 UNION keywords (UNION stacking)
```

**Defence layers:**

```
Raw SQL from Claude
        │
        ▼
[Check 1] Empty → REJECT
        │
        ▼
[Check 2] Leading keyword blocked → REJECT "Statement type 'DROP' is not permitted."
        │
        ▼
[Check 3] Inline blocked keyword (word boundary) → REJECT "Query contains a prohibited operation."
        │
        ▼
[Check 4] Suspicious structure → REJECT "Query structure is not permitted."
        │
        ▼
     ALLOWED → passed to execute_sql
```

**Modular:** Yes. Pure function, no DB knowledge, testable in isolation.

**Why layered:** Check 2 catches obvious attacks. Check 3 catches embedded keywords like `SELECT 1; DROP TABLE`. Check 4 catches obfuscation patterns.

---

### 4.15 `scripts/m8_output_sanitiser.py` — Output Sanitiser (Security Layer)

**Purpose:** Cleans errors and result data before reaching the user. Never exposes DuckDB internals.

**Code blocks:**

```
sanitise_error(raw_error) → str
    │
    ├── _ERROR_NORMALISATION lookup:
    │       "does not exist" → "table not found"
    │       "no such table"  → "table not found"
    │       "no such column" → "column not found"
    │       "unknown column" → "column not found"
    │
    ├── SAFE_ERROR_SUBSTRINGS check:
    │       "syntax error", "column not found", "table not found",
    │       "ambiguous column", "division by zero", "type mismatch", "time limit"
    │       → Extract 120-char excerpt, strip file paths & line numbers
    │
    └── None matched → GENERIC_ERROR_MESSAGE
            "The query could not be completed. Please try rephrasing."

check_null_rates(df) → list[str]
    ← Warns per column if null% > NULL_FLAG_THRESHOLD (20%)

sanitise_result(result) → dict
    ← Never mutates input (creates new dict)
    ← Adds "warnings" key always
```

**What it addresses:**
- Prevents DuckDB stack traces, file paths, line numbers from being shown to users
- Normalises DuckDB's verbose error phrasing to canonical safe strings
- Surfaces data quality warnings (null rates) as user-visible warnings

---

### 4.16 `scripts/m10_benchmark.py` — Benchmark Evaluation Suite

**Purpose:** Evaluates SQL generation quality across 30 pre-defined questions with scoring.

**Code blocks:**

```
BENCHMARK_QUESTIONS: list[dict]
    ← 30 questions across categories:
        single_table, join_query, aggregation, filter,
        date_time, subquery, blocked_ddl

run_benchmark(client, con, system_prompt) → BenchmarkReport
    ← For each question:
        1. attempt_with_retry() → result
        2. Score: sql_correct, db_executed, result_sane
        3. Overall: PASS / PARTIAL / FAIL / BLOCKED_OK / BLOCKED_FAIL
    ← Aggregates into BenchmarkReport (Pydantic)

save_results(report, path)
    ← report.to_json() → docs/benchmark_results.json

compare_to_baseline(report)
    ← BenchmarkReport.from_json() → loads baseline
    ← Compares sql_pct, db_pct, pass_pct, avg_latency_ms

print_results_table(report)
    ← Tabular output per question with PASS/FAIL markers
```

**Scoring rubric:**

```
sql_correct:  SQL was generated (not CANNOT_ANSWER and not empty)
db_executed:  execute_sql returned success=True
result_sane:  validate_result_sanity returned True

PASS         = sql_correct + db_executed + result_sane
PARTIAL      = sql_correct + db_executed (no sane check passed)
FAIL         = sql did not execute
BLOCKED_OK   = question was a blocked DDL test and was correctly blocked
BLOCKED_FAIL = question was a blocked DDL test but was not blocked
```

---

## 5. Memory System — Detailed Explanation

### Memory Type

This project uses **in-process session memory** (not persistent/file-based). Memory lives in `st.session_state.memory` for the duration of a browser session. It is a Python list of dicts.

### Memory Structure

Each element (a "turn") stores:

```python
{
    "question": "Top departments by reorder rate?",
    "sql":      "SELECT d.department, AVG(od.reordered) AS reorder_rate ...",
    "summary":  "Top 5 results for department by reorder_rate:\n 1. produce: 66.2%...",
    "success":  True
}
```

The **summary** is the key differentiator from simple history. It is a compact natural-language rendering of the result data, enabling the system to answer "why?" follow-ups without re-querying the database.

### Memory Flow

```
User submits question
        │
        ▼
attempt_with_retry() → result dict
        │
        ▼
add_turn(memory, question, result["sql"], result)
        │
        ├── result success? → summarise_result(df, question, sql)
        │       ├── 1×1 → scalar summary   "Result: 3,421,083."
        │       ├── N×2 → ranked list      "1. produce: 66.2%..."
        │       └── else → generic         "25 rows × 4 cols..."
        │
        └── result failure? → "Query failed: {error_preview}"
                │
                └── {question, sql, summary, success} appended to memory
```

### Memory Consumption in API Calls

`build_messages()` converts memory into Claude API message format:

```
memory turn → two API messages:
  {"role": "user",      "content": "Top departments by reorder rate?"}
  {"role": "assistant", "content": "SELECT... \n\n[Result: 1. produce: 66.2%...]"}
```

**Dynamic window:** The number of memory turns included scales with question complexity:

```
Normal questions:    last 3 turns → 6 messages (+ current = 7 total)
Multistep questions: last 5 turns → 10 messages (+ current = 11 total)
```

Trigger keywords for extended window (from `config.py`):
`compare, versus, vs, difference between, trend, over time, step by step, first, then, next, after that, following, subsequently`

### Memory Usage by Module

| Module | How it uses memory |
|---|---|
| `m3_4_error_recovery.py` | `build_messages(question, memory)` → Claude sees prior Q&A as context for SQL generation |
| `m5_planner.py` | `build_planner_message(question, memory)` → last 2 turns for intent classification context |
| `m5_conversational.py` | `build_conversational_context(question, memory)` → last 3 turn summaries for answering follow-ups |
| `app.py` | `get_context_summary(memory)` → sidebar display |

---

## 6. Agentic Architecture Explanation

This system implements a **multi-agent pipeline with intent routing** (Architecture 2). It is not a loop-based agent; it is a **deterministic router with specialised sub-agents** per intent type.

### Agent Roles

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         ORCHESTRATOR (app.py)                          │
│  Owns: session state, DB connection, client, memory, UI rendering      │
│  Delegates to: Planner, then routes to appropriate executor             │
└────────────────────────┬────────────────────────────────────────────────┘
                         │
          ┌──────────────▼──────────────┐
          │      PLANNER AGENT          │
          │      (m5_planner.py)        │
          │                             │
          │  Claude call (forced        │
          │  tool_use) → PlannerOutput  │
          │                             │
          │  Returns: PlannerDecision   │
          │  {intent, reason, subqs}    │
          └──┬────────┬────────┬────────┘
             │        │        │
    ┌─────────▼──┐  ┌─▼──────┐  ┌▼─────────────────────┐
    │  CANNOT    │  │ CONV.  │  │   DATA QUERY AGENT    │
    │  ANSWER    │  │ AGENT  │  │   (m3_*.py pipeline)  │
    │            │  │        │  │                        │
    │ Fixed resp.│  │ Claude │  │ SQL Gen → Execute      │
    │ No API call│  │ free   │  │ → Retry on failure     │
    └────────────┘  │ text   │  └──────────┬─────────────┘
                    │ call   │             │
                    └────────┘             ▼
                                  ┌────────────────────┐
                                  │   CRITIC AGENT     │
                                  │   (m5_critic.py)   │
                                  │                    │
                                  │ Sanity validation  │
                                  │ + narrative insight│
                                  │ (Claude tool_use)  │
                                  └────────────────────┘
```

### Agent Communication Protocol

Agents communicate via **plain Python dicts and dataclasses** — not message queues or function-calling chains. The Orchestrator (`app.py`) is the only component that reads from or writes to `st.session_state`. Sub-agents receive only what they need and return typed results.

```
Planner receives:  (client, question: str, memory: list[dict])
Planner returns:   PlannerDecision(intent, reason, subqueries)

Critic receives:   (client, question: str, result: dict)
Critic returns:    CriticValidation(sane, issues, narrative: CriticOutput|None)

Conversational receives: (client, question: str, memory: list[dict])
Conversational returns:  str  (plain text answer)

SQL Pipeline receives: (client, con, system_prompt, question, memory)
SQL Pipeline returns:  dict {success, data, sql, row_count, error, warnings}
```

### Structured Output via Tool Use

Both the Planner and Critic use **Anthropic's forced tool use pattern** to guarantee structured output:

```python
# Forces Claude to return a JSON object matching the Pydantic schema
tool_choice={"type": "tool", "name": "classify_intent"}

# Extracts the structured result
tool_block = next(b for b in response.content if b.type == "tool_use")
output = PlannerOutput(**tool_block.input)   # Pydantic validates
```

This ensures the LLM can never return free-text where structured data is expected. The schema is generated directly from the Pydantic model: `PlannerOutput.model_json_schema()`.

### Multistep Handling

When intent is `multistep`, the Orchestrator iterates over the Planner's `subqueries` list, running each through the full SQL pipeline independently, storing each turn in memory as it goes:

```
question: "Compare reorder rates between produce and frozen foods"
    │
    ▼
Planner: intent=multistep
         subqueries=["Reorder rate for produce", "Reorder rate for frozen foods"]
    │
    ▼
for subq in subqueries:
    result = attempt_with_retry(subq)
    memory = add_turn(memory, subq, result["sql"], result)
    │
    ▼
critique(last_successful_result)
```

### Fallback Safety

Every agent has a graceful degradation path:

| Agent | Failure | Fallback |
|---|---|---|
| Planner | API error / ValidationError / no tool block | `PlannerDecision(intent="data_query")` |
| Critic | API error / ValidationError | `_FALLBACK_CRITIC` (CriticOutput with generic text) |
| Conversational | API error | `_FALLBACK` string |
| SQL Generator | CANNOT_ANSWER response | `_CANNOT_ANSWER_RESULT` dict |
| Executor | timeout | sanitised timeout error |
| Executor | DB error | M8-sanitised error |

---

## 7. Security Architecture

Two dedicated modules form a security perimeter around DuckDB:

```
Claude generates SQL
        │
        ▼
┌───────────────────────┐
│  M7 INPUT VALIDATOR   │  ← Before DB touch
│                       │
│  Empty?     → REJECT  │
│  DDL/DML?   → REJECT  │
│  File ops?  → REJECT  │
│  Multi-stmt?→ REJECT  │
│  UNION spam?→ REJECT  │
└──────────┬────────────┘
           │ ALLOWED
           ▼
┌───────────────────────┐
│    DuckDB (read-only) │  ← DB_READ_ONLY=True, 2GB limit, 15s timeout
└──────────┬────────────┘
           │ raw result
           ▼
┌───────────────────────┐
│  M8 OUTPUT SANITISER  │  ← After DB response
│                       │
│  Normalise errors     │
│  Strip file paths     │
│  Flag null rates      │
│  Suppress internals   │
└───────────────────────┘
           │ safe result
           ▼
         USER
```

DuckDB is opened `read_only=True` as a second line of defence — even if M7 misses a write statement, the DB connection itself rejects it.

---

## 8. Modularity Assessment

### Highly Modular (can swap independently)

| Module | Why |
|---|---|
| `m6_summariser.py` | Interface is `summarise_result(df, q, sql) → str`. Could swap to LLM-based summariser with zero other changes |
| `m3_2_sql_generator.py` | Interface is `generate_sql(client, system, messages) → str`. Could swap to GPT-4 or a fine-tuned model |
| `m4_renderer.py` | Adding a new chart type = add one `render_X()` function and one entry in `_RENDER_MAP` |
| `m5_schemas.py` | All schemas centralised; validation logic lives in the schema, not scattered across callers |
| `m7_input_validator.py` | Pure function, no dependencies on DB or API |
| `m8_output_sanitiser.py` | Pure function, no dependencies on DB or API |

### Less Modular (tightly coupled)

| Module | Issue | How to improve |
|---|---|---|
| `config.py` | Imports from `scripts.m5_schemas` at module level — creates circular-risk and makes `config.py` dependent on the scripts package | Move the schema import to a separate `architecture_config.py` or use lazy imports |
| `m3_2_sql_generator.py` | Duplicates `MODEL` and `MAX_TOKENS` (also in `config.py`) | Always read from `config.py` |
| `m4_chart_selector.py` & `m4_renderer.py` | Both define `_TEMPORAL_KW` and `_METRIC_KW` independently | Extract to a `m4_constants.py` |
| `m3_4_error_recovery.py` | `MAX_RETRIES=3` is a hardcoded constant, not in `config.py` | Move to `config.py` |
| `app.py` | All Architecture 2 routing is inline in one large `if question:` block | Extract to an `orchestrator.py` function |

---

## 9. Data Flow Summary

```
User types: "Top departments by reorder rate?"
                │
                ▼ (Architecture 2)
         Planner → intent: data_query
                │
                ▼
         M3.1: system_prompt = schema_context + SQL rules
                │
                ▼
         M6 memory.build_messages() → [user/assistant turns] + current question
                │
                ▼
         M3.2: Claude API → SQL string
                │
                ▼
         M7: validate(sql) → allowed
                │
                ▼
         M3.3: inject_limit → threaded DuckDB execution
                │
                ▼
         M8: sanitise_result() → {success, data, sql, warnings}
                │
                ▼
         M5 Critic: validate_result_sanity() + generate_narrative()
                │                      │
                ▼                      ▼
         CriticValidation         CriticOutput (insight)
                │
                ▼
         M6: add_turn() → M6 Summariser → summary text → stored in memory
                │
                ▼
         M4 Chart Selector: detect_chart_type(df)
                │
                ▼
         M4 Renderer: render_chart(df, chart_type, title) → Plotly figure
                │
                ▼
         Streamlit: chart + dataframe + insight panel + warnings
```

---

## 10. Key Design Decisions & Salient Features

| Feature | Design Decision |
|---|---|
| **Architecture switching** | Single `ARCHITECTURE` constant in `config.py` — no code changes required to switch |
| **Forced tool use** | Planner and Critic both use `tool_choice={"type":"tool"}` — Claude cannot return free text where a schema is expected |
| **Immutable benchmark records** | `model_config={"frozen": True}` on `BenchmarkQuestionResult` — scores cannot be accidentally modified |
| **No state mutation in M8** | `sanitise_result` copies the input dict — original is never modified |
| **Daemon threads** | `thread.daemon=True` in executor — process cannot hang on a slow query |
| **Schema-driven prompts** | `PlannerOutput.model_json_schema()` generates the Anthropic tool schema directly from the Pydantic class — schema and validation are always in sync |
| **Programmatic summarisation** | M6 Summariser uses zero API calls — memory stays cheap to build |
| **Two-layer security** | M7 rejects before DB touch; DB connection is read-only as a second layer |
| **Summary in memory (not raw data)** | Memory stores text summaries, not DataFrames — keeps context window small and usable for follow-up reasoning |
| **Self-tests in every module** | Each `__main__` block tests the module in isolation — no pytest needed for smoke-testing |

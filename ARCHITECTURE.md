# Architecture

## Overview

The Conversational BI Agent is built as a layered pipeline. Each layer has a single responsibility and is independently testable. The two architectures share the same data and execution layers — Architecture 2 adds a classification and validation wrapper around Architecture 1's core SQL pipeline.

```
┌─────────────────────────────────────────────────────┐
│                   Streamlit UI (app.py)              │
├─────────────────────────────────────────────────────┤
│         Architecture 2 — Planner + Critic           │
│    m5_planner  │  m5_conversational  │  m5_critic   │
├─────────────────────────────────────────────────────┤
│         Architecture 1 — Core SQL Pipeline          │
│   m3_1_prompt  │  m3_2_generator  │  m3_3_executor  │
│                    m3_4_error_recovery               │
├─────────────────────────────────────────────────────┤
│              Result Layer                           │
│      m4_chart_selector  │  m4_renderer              │
│      m6_memory          │  m6_summariser             │
├─────────────────────────────────────────────────────┤
│              Safety Layer                           │
│      m7_input_validator  │  m8_output_sanitiser     │
├─────────────────────────────────────────────────────┤
│              Schema Layer                           │
│   m2_1_description │ m2_2_joins │ m2_3_prompt_builder│
├─────────────────────────────────────────────────────┤
│              Data Layer                             │
│        DuckDB (instacart.db)  │  config.py          │
└─────────────────────────────────────────────────────┘
```

---

## Architecture 1 — Direct SQL

The baseline pipeline. A user question goes directly to the LLM with schema context, SQL is generated and executed, results are rendered.

```
User question
      │
      ▼
M7 input validator ──── blocked ──→ user-facing error
      │ allowed
      ▼
M3.1 prompt builder
(schema context + question + history)
      │
      ▼
M3.2 SQL generator (Claude API)
      │
      ▼
M3.3 executor (DuckDB, read-only, 15s timeout)
      │
      ├── success ──→ M8 output sanitiser
      │                     │
      │               M4 chart selector + renderer
      │                     │
      └── failure ──→ M3.4 error recovery (up to 3 retries)
                            │
                      M8 output sanitiser
```

Enable with:
```python
# config.py
ARCHITECTURE = 1
```

---

## Architecture 2 — Planner + Critic

Wraps Architecture 1. Adds intent classification before SQL generation and result validation + narrative generation after execution. The M3 pipeline is unchanged.

```
User question
      │
      ▼
M7 input validator ──── blocked ──→ user-facing error
      │ allowed
      ▼
M5 Planner (Claude API — structured output)
      │
      ├── data_query ──────→ M3 pipeline (Architecture 1)
      │                            │
      │                      M5 Critic
      │                      (validates result + generates narrative)
      │
      ├── conversational ──→ M5 conversational handler
      │                      (answers from M6 memory — no SQL)
      │
      ├── multistep ───────→ M3 pipeline × N subqueries sequentially
      │                            │
      │                      M5 Critic on final result
      │
      └── cannot_answer ──→ graceful rejection message
```

Enable with:
```python
# config.py
ARCHITECTURE = 2
```

Switching between architectures requires no code changes — only the `config.py` flag.

---

## Module reference

### Data layer

**`config.py`** — single source of truth for all configuration. Database paths, model names, table lists, FK relationships, size hints, semantic notes, query rules, security settings, and architecture flag all live here. Swapping datasets means updating this file and regenerating `schema_context.txt`.

**`scripts/build_database.py`** — loads the 6 Instacart CSVs into DuckDB, establishes the `order_details` unified view (UNION ALL of prior + train joined to orders), and creates the persistent `models/instacart.db` file.

**`scripts/validate.py`** — asserts row counts match expected values, scans for unexpected nulls, checks referential integrity across all FK relationships.

---

### Schema layer (M2)

**`m2_1_table_description.py`** — parses `docs/schema_metadata.txt` for structural information (columns, types, PKs), then queries DuckDB live for sample values only. Returns a `schema_raw` dict used by M2.3.

**`m2_2_join_relationships.py`** — defines `FK_RELATIONSHIPS` and `TABLE_SIZE_HINTS` (imported from `config.py`). Generates human-readable join path strings and validates referential integrity.

**`m2_3_prompt_builder.py`** — assembles the final `schema_context.txt` from the raw metadata, join descriptions, and query rules. This string becomes the LLM system prompt for every session.

**`m2_4_validate_prompt.py`** — sends 10 benchmark questions to the Claude API using `schema_context.txt` as the system prompt. Checks that generated SQL uses correct table names, join paths, and encodings. Must pass before M3 is used.

**`m2_run.py`** — orchestrates M2.1 → M2.2 → M2.3 → validates → saves `docs/schema_context.txt`.

---

### Core pipeline (M3)

**`m3_1_prompt_builder.py`** — loads `schema_context.txt`, appends SQL generation instructions, and builds the messages list for the Claude API including conversation history (capped at last 3 turns by default, expanded to 5 for multi-step questions).

**`m3_2_sql_generator.py`** — calls the Claude API and returns a clean SQL string. Strips markdown fences defensively. Returns the string `"CANNOT_ANSWER"` if the model signals the question is out of scope.

**`m3_3_executor.py`** — executes SQL against DuckDB using a read-only connection with a 2GB memory cap and 15-second timeout enforced via threading. Injects `LIMIT 1000` on SELECT queries that lack one. Returns a structured result dict — never raises to the caller.

**`m3_4_error_recovery.py`** — implements up to 3 total attempts. On failure, feeds the original question + failed SQL + DuckDB error back to the LLM and asks for a corrected query. Returns the first successful result or the final failure after 3 attempts.

**`m3_run.py`** — terminal REPL interface. Same pipeline as the Streamlit app, useful for testing without the UI.

---

### Result layer (M4, M6)

**`m4_chart_selector.py`** — inspects the result dataframe's column types and cardinality to recommend a chart type. Returns the recommended chart plus a list of all valid alternatives the user can choose from in the UI. Auto-selects: bar for categorical + numeric, line for temporal + numeric, pie for proportional results, histogram for single numeric columns, heatmap for two categoricals + one numeric.

**`m4_renderer.py`** — takes a dataframe and chart type string and returns a Plotly figure. All figures use transparent backgrounds and light text for compatibility with Streamlit's dark theme. The `apply_theme()` helper is applied to all non-table chart types.

**`m6_memory.py`** — manages conversation history as a list of turn dicts, each containing the question, SQL, result summary, and success flag. `build_messages()` converts memory to the Claude API messages format. Dynamically expands the history window from 3 to 5 turns when multi-step keywords are detected in the current question.

**`m6_summariser.py`** — generates programmatic text summaries of result dataframes without API calls. Routes to specialist summarisers based on result shape: scalar (single value), two-column top-N, single-row multi-column, or generic fallback. Formats numbers intelligently — commas for large integers, percentages for 0–1 floats.

---

### Safety layer (M7, M8)

**`m7_input_validator.py`** — validates generated SQL before execution. Blocks DDL (`DROP`, `CREATE`, `ALTER`, `TRUNCATE`), DML writes (`DELETE`, `INSERT`, `UPDATE`), and file operations (`COPY`, `EXPORT`). Also detects multi-statement SQL and suspicious comment-based obfuscation patterns. Returns a `ValidationResult` dataclass — never modifies the SQL.

**`m8_output_sanitiser.py`** — sanitises error messages before they reach the UI, stripping DuckDB internals, file paths, and line numbers. Only shows error substrings that are safe and descriptive (syntax errors, missing columns). Checks result dataframes for high null rates and surfaces warnings. Adds a `warnings` key to every result dict.

The read-only DuckDB connection (`read_only=True`) is the primary write protection — it operates at the driver level and cannot be bypassed by application code regardless of what SQL is submitted.

---

### Architecture 2 agents (M5)

**`m5_schemas.py`** — all Pydantic schemas in one file. `PlannerOutput` (intent classification), `CriticOutput` (narrative insight), `BenchmarkQuestionResult` (immutable benchmark record), `BenchmarkReport` (full benchmark with serialisation). All schemas use field validators to enforce business rules — `followup` always ends with `?`, `subqueries` is always empty for non-multistep intents, numeric fields have range constraints.

**`m5_planner.py`** — classifies user intent using Claude API structured output. Forces `PlannerOutput` schema via `tool_choice`. Defaults safely to `data_query` on any API or validation error. Logs every decision with intent and reason for debugging.

**`m5_conversational.py`** — handles questions classified as `conversational`. Builds a context string from the last 3 memory turns including result summaries, then generates a free-text answer via Claude. No SQL is generated — the answer comes from what the prior queries already returned.

**`m5_critic.py`** — runs after SQL execution. Validates result sanity (zero rows, high null rates, implausible zero aggregates) and generates a `CriticOutput` narrative using Claude structured output. Narrative is skipped for scalar results (single row). Falls back gracefully on API errors without blocking result display.

---

### Evaluation (M10)

**`m10_benchmark.py`** — runs 30 benchmark questions covering 7 categories. Each question scores three independent dimensions: SQL correctness (keyword checks), DB execution (runs without error), and result sanity (values within expected ranges). Saves results as `BenchmarkReport` JSON. Compares to saved baseline on subsequent runs. Sets exit code 1 if full pass rate is below 80%.

Note: the benchmark calls `attempt_with_retry` directly, bypassing the Planner. Architecture 1 and Architecture 2 therefore produce identical benchmark scores. A future enhancement would route benchmark questions through the Planner to measure classification accuracy separately.

---

## Data flow — schema to prompt

```
DuckDB (live)                docs/schema_metadata.txt
      │                              │
      │ sample values                │ column structure
      │ (M2.1 fetches live)          │ (M2.1 parses file)
      └──────────────┬───────────────┘
                     ▼
              schema_raw dict
                     │
                     ▼
            M2.2 join descriptions
            M2.3 prompt assembly
            M2.3 query rules
                     │
                     ▼
         docs/schema_context.txt
         (LLM system prompt — generated once, used every session)
```

---

## Data flow — question to result

```
User types question
         │
         ▼
M7 validates (SQL keyword check)
         │
         ▼ (Architecture 2 only)
M5 Planner classifies intent
         │
    ┌────┴────┬──────────────┐
    ▼         ▼              ▼
data_query  convers.    multistep
    │         │              │
    ▼         ▼              │
M3.1 build  M5 answer    repeat M3
M3.2 gen    from memory  for each
M3.3 exec               subquery
M3.4 retry      │              │
    │           └──────┬───────┘
    ▼                  ▼
M8 sanitise      M5 Critic
M6 summarise     (validate + narrative)
    │                  │
    └──────────────────┘
                 ▼
    M4 chart selector + renderer
                 ▼
    Streamlit UI (chart + table + insight panel)
```

---

## Modularity

The system is designed to work on any DuckDB dataset by changing `config.py` only:

| What changes | Effort |
|---|---|
| Add a new table to existing DB | Add COPY in `build_database.py`, add FK in `config.py`, re-run `m2_run.py` |
| Switch to a new dataset entirely | Rewrite `build_database.py`, update `FK_RELATIONSHIPS` + `SEMANTIC_NOTES` + `QUERY_RULES` in `config.py`, re-run `m2_run.py` |
| Change LLM model | Update `MODEL` in `config.py` |
| Change architecture | Update `ARCHITECTURE` in `config.py` |
| Adjust memory or timeout | Update `DUCKDB_MEMORY_LIMIT` or `QUERY_TIMEOUT_SECONDS` in `config.py` |

All M3, M4, M5, M6, M7, M8 modules work without modification on any dataset.

---

## Security model

Three independent layers protect the database:

**Layer 1 — Intent check** (partial implementation)
Detects destructive intent in user questions before the LLM is called. Known gap — see limitations in README.

**Layer 2 — SQL validator** (`m7_input_validator.py`)
Inspects generated SQL for blocked keywords and suspicious patterns before DuckDB execution. Returns a structured `ValidationResult` — never silently passes a blocked query.

**Layer 3 — Read-only connection** (primary protection)
DuckDB is opened with `read_only=True`. Any write operation — regardless of how SQL was generated or what slipped through Layer 2 — is rejected at the driver level. This cannot be bypassed by application code.

All three layers are independent. Layer 3 alone makes the database safe. Layers 1 and 2 provide better user experience by surfacing clear error messages before the request reaches the database.

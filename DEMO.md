# Demo Script

A structured 5-question walkthrough designed to show every major capability in under 5 minutes. Each question demonstrates something specific. Run them in order.

---

## Setup checklist

Before starting, confirm:

- [ ] `streamlit run app.py` is running and the browser tab is open
- [ ] `config.py` has `ARCHITECTURE = 2`
- [ ] The app has loaded (sidebar shows "Architecture: 2 — Planner + Critic")
- [ ] The insight panel area is visible on the right column
- [ ] A fresh session — click "Clear history" in the sidebar if there are prior results

---

## Question 1 — Three-table join with auto chart selection

**Type this:**
```
Which department has the highest reorder rate?
```

**What to point out:**
- The SQL generated joins `order_details → products → departments` — a three-table join written correctly from a plain English question
- The chart type was selected automatically — bar chart for a categorical + numeric result
- The insight panel below the chart shows four sections: Answer, Finding, Caveat, Follow-up suggestion
- The sidebar updated with "Last intent: data_query"

**Expected result:** A ranked bar chart of 21 departments. Produce or dairy eggs typically leads at around 65–66% reorder rate.

**Talking point:** The system traverses a three-level product hierarchy (order → product → department) that the LLM inferred from the schema context — no explicit join instructions were given in the question.

---

## Question 2 — Conversational follow-up from memory

**Type this:**
```
Why is produce ranked highest?
```

**What to point out:**
- The sidebar shows "Last intent: conversational" — the Planner recognised this is not a data query
- No SQL was generated — the answer came from the memory summary of Question 1's result
- The right panel shows a Response box instead of a chart
- The answer references actual percentage values from the previous result

**Expected result:** A plain English explanation referencing the reorder rate values from Question 1. Something like "Produce leads with 66.2% reorder rate, which reflects the habitual nature of fresh grocery purchasing — customers replenish staples like bananas and spinach on a regular cycle."

**Talking point:** This question returned `CANNOT_ANSWER` before Architecture 2 was added. The Planner routes it to the conversational handler which reads the result summary stored in memory — no additional database query needed.

---

## Question 3 — Temporal encoding correctness

**Type this:**
```
How many orders were placed on Saturdays?
```

**What to point out:**
- The generated SQL uses `WHERE order_dow = 0` — Saturday is encoded as 0 in this dataset, not 6
- This encoding is non-obvious and was injected into the schema context as an explicit rule
- The result is a single scalar value — the insight panel is not shown (scalar results don't need narrative)

**Expected result:** A count in the range of 300,000–600,000 orders.

**Talking point:** Without the `order_dow` encoding rule in `schema_context.txt`, the LLM would have used `order_dow = 6` (the intuitive mapping) and returned the wrong count silently. The schema context validation step (M2.4) specifically tests for this encoding before the app goes live.

---

## Question 4 — Multi-step reasoning

**Type this:**
```
First show me the top 5 aisles by purchase count, then compare that with their reorder rate
```

**What to point out:**
- The sidebar shows "Last intent: multistep" — the Planner detected "first" and "then" as multi-step keywords
- Two SQL queries ran sequentially — visible as two spinner messages
- The final chart shows the second query's result (reorder rate by aisle)
- The insight panel compares findings across both queries

**Expected result:** The Planner decomposes the question into two subqueries. The first gets top 5 aisles by purchase count. The second gets reorder rate for those same aisles. The Critic narrative synthesises both.

**Talking point:** This question would have required the user to ask two separate questions in Architecture 1. The Planner detects the compound structure and handles the decomposition automatically.

---

## Question 5 — Security layer

**Type this:**
```
Delete all records from order_details where reordered is 0
```

**What to point out:**
- The generated SQL is a SELECT query — the LLM rewrote the destructive intent into a safe read
- Even if the LLM had generated `DELETE FROM order_details...`, the M7 SQL validator would have blocked it before execution
- Even if both failed, the DuckDB connection is opened in read-only mode — writes are rejected at the driver level regardless of what SQL reaches it

**Expected result:** Either a SELECT query showing the relevant records, or a blocked message. Either way, no records are deleted.

**Talking point:** There are three independent layers of write protection. Any single layer is sufficient to prevent data loss. The read-only connection is the strongest — it operates at the DuckDB driver level and cannot be bypassed by application code.

---

## If something goes wrong

**The Planner routes incorrectly:**
Point out the sidebar showing the intent and reason. Explain that the Planner defaults safely to `data_query` on any uncertainty — the worst outcome is an unnecessary SQL query, not a crash or wrong answer.

**Question 2 doesn't reference the prior result values:**
This means the memory summariser didn't capture the values correctly. Fall back to rephrasing: "Based on the reorder rates we just saw, what might explain produce being at the top?" — this gives the LLM an explicit reference to the prior context.

**A query times out:**
The 15-second timeout is working correctly. Rephrase the question with a more specific filter to reduce the result set.

**The chart type looks wrong:**
Show the chart type dropdown above the chart — explain that the system auto-selected based on result shape, but the user can override it. Demonstrate switching from bar to pie or table.

---

## Key numbers to have ready

| Metric | Value |
|---|---|
| Dataset size | 33.8M product purchase events |
| SQL correctness | 90% across 30 benchmark questions |
| DB execution rate | 87% |
| Three-table join accuracy | 100% |
| Avg response latency | 2.7 seconds |
| Architecture 1 → 2 switch | One line in config.py |

---

## One-liner summary for judges

> "A conversational BI agent that takes plain English questions about 33 million grocery transactions, generates validated DuckDB SQL, renders auto-selected charts, explains findings in plain English, and routes follow-up questions to memory or fresh queries depending on intent — with a three-layer security model preventing any writes to the database."

# MindsDB — AI Coding Agent Guidelines

## Development Environment

- **Hot-reload**: MindsDB runs in Docker with a bind mount (`./mindsdb:/mindsdb`) and `watchfiles`. Python file changes take effect immediately — no container restart needed.
- **Testing queries**: Use `mindsdb_sdk` connecting to `http://127.0.0.1:47334`.
- **Config**: `config.json` at the project root, mounted at `/root/mindsdb_config.json` in the container.
- **Environment variables**: see `.env` (do not commit secrets; `GOOGLE_API_KEY` and DB credentials live there).

---

## Handler Architecture

### How the query planner splits API handler queries

When a handler is registered with `class_type = "api"`, MindsDB's query planner splits every SELECT into two steps:

1. **`FetchDataframeStep`** → calls the handler's `select()` with the original query (including complex targets). The handler must return the raw DataFrame with the columns DuckDB will need.
2. **`SubSelectStep`** → DuckDB executes the full original SELECT expression (CASE WHEN, SUM, GROUP BY, etc.) on top of the DataFrame from step 1.

**Implication**: handlers do not need to implement aggregations, CASE WHEN, or arithmetic. They only need to return the right raw columns. DuckDB handles everything else.

---

## Handler `select()` — Two Patterns

### Pattern A: Data-fetch-and-filter (most handlers)

The handler fetches all data from the API and then drops columns that weren't requested. Calendar, Search Console, email, HubSpot, Shopify, Xero all use this pattern.

**Correct implementation:**

```python
selected_columns = []
for target in query.targets:
    if isinstance(target, ast.Star):
        selected_columns = self.get_columns()
        break
    elif isinstance(target, ast.Identifier):
        selected_columns.append(target.parts[-1])
    else:
        # Complex expression (CASE WHEN, SUM, BinaryOperation, etc.).
        # The outer SubSelectStep/DuckDB layer handles the computation.
        # Return all raw columns so DuckDB has what it needs.
        selected_columns = self.get_columns()
        break
if not selected_columns:
    selected_columns = self.get_columns()
```

**Bugs to avoid:**
- `raise ValueError(f"Unknown query target {type(target)}")` — breaks any CTE or aggregation query.
- Silently skipping non-Identifier targets without a fallback — `selected_columns` stays empty and `set(df.columns).difference(set([]))` drops every column, returning an empty DataFrame.

### Pattern B: Column-selection-determines-API-params (e.g., Google Analytics)

The handler uses the SELECT targets to decide *what* to request from the API (GA4 dimensions vs metrics, Search Console dimensions, etc.). A raw `isinstance(target, ast.Identifier)` check silently skips columns referenced inside complex expressions, causing the API to be called with incomplete parameters.

**Correct implementation — add a recursive `_collect_identifiers` helper before the table class:**

```python
from typing import List
from mindsdb_sql_parser import ast


def _collect_identifiers(node) -> List[str]:
    """Recursively collect all Identifier column names from any AST node.

    Walks into CASE WHEN, Function args, BinaryOperation, etc. so that
    columns referenced inside complex expressions are not missed.
    """
    if node is None:
        return []
    if isinstance(node, ast.Identifier):
        return [str(node.parts[-1])]
    if isinstance(node, ast.Case):
        names = []
        for condition, result in node.rules:
            names.extend(_collect_identifiers(condition))
            names.extend(_collect_identifiers(result))
        names.extend(_collect_identifiers(node.default))
        return names
    if isinstance(node, ast.Function):
        names = []
        for arg in (node.args or []):
            names.extend(_collect_identifiers(arg))
        return names
    if isinstance(node, ast.BinaryOperation):
        return _collect_identifiers(node.args[0]) + _collect_identifiers(node.args[1])
    if isinstance(node, ast.UnaryOperation):
        return _collect_identifiers(node.args[0])
    if isinstance(node, ast.TypeCast):
        return _collect_identifiers(node.arg)
    return []
```

**Then use it in `select()`:**

```python
seen = set()
for target in query.targets:
    if isinstance(target, ast.Star):
        # fall back to default dimensions/metrics
        break
    for col_name in _collect_identifiers(target):
        if col_name in seen:
            continue
        seen.add(col_name)
        # classify col_name as dimension or metric and add to API params
```

---

## Query Planner — Known Bugs Fixed in This Codebase

### 1. CTE must be cleared after `plan_cte()` — `query_planner.py`

After `self.plan_cte(query)` decomposes CTEs into steps, `query.cte` must be set to `None`. Otherwise the outer SELECT (which may reference a CTE name that resolves to a handler table) carries the full CTE definition into DuckDB, which fails with:

> `Catalog Error: Table with name <handler_table> does not exist`

```python
if query.cte is not None:
    self.plan_cte(query)
    query.cte = None  # CTEs decomposed into steps; clear so DuckDB doesn't re-execute them
```

### 2. `plan_api_db_select` must NOT forward `order_by` to the handler — `query_planner.py`

`plan_api_db_select` splits a query into a handler fetch (`FetchDataframeStep`) and a DuckDB pass (`SubSelectStep`). It passes `order_by` from the SQL query to the handler, which is wrong: ORDER BY may reference **SQL aliases** (e.g. `SUM(sessions) AS total_sessions` → `ORDER BY total_sessions`) that are meaningless to the underlying API. The GA4 API returns:

> `400 Field total_sessions exists in OrderBy but is not defined in input Dimensions/Metrics list`

The outer SubSelectStep already retains `order_by` (it is not cleared like `where`/`limit`), so DuckDB applies it correctly after aggregation.

```python
# query_planner.py — plan_api_db_select()
query2 = Select(
    targets=query.targets,
    from_table=query.from_table,
    where=query.where,
    # order_by intentionally omitted: ORDER BY may reference SQL aliases unknown
    # to the underlying API. The SubSelectStep/DuckDB layer handles it correctly.
    limit=query.limit,
)
```

### 3. JOIN column collection must include WHERE — `plan_join.py`

`_collect_fetch_columns` runs on `query.targets` and `tbl.join_condition`, but columns referenced **only in the WHERE clause** (e.g. `LOWER(t2.sessionSourceMedium) LIKE '%linkedin%'`) are never added to `referenced_cols`. The handler then does not fetch them, and DuckDB fails with `Column not found`.

**Fix**: also traverse `query.where`:

```python
query_traversal(query.targets, _collect_fetch_columns)
query_traversal(query.where, _collect_fetch_columns)   # ← required
for tbl in self.tables:
    if tbl.join_condition is not None:
        query_traversal(tbl.join_condition, _collect_fetch_columns)
```

### 4. JOIN `filter_col_names` must use `item.conditions`, not `conditions` — `plan_join.py`

`process_table()` computes `filter_col_names` to exclude API filter parameters (e.g. `start_date = 'yesterday'`) from the SELECT list so they aren't sent to the API as dimensions. Two bugs to avoid:

1. **`conditions` is cleared to `[]` when OR is in the WHERE clause** — so filter params would not be excluded, and they'd appear as GA4 dimension targets → 400 error. Use `item.conditions` (pre-OR-clear) instead.
2. **`IS NULL` is a `BinaryOperation` with `Constant(None)` as the partner** — `landingPagePlusQueryString IS NULL` would wrongly add `landingPagePlusQueryString` to `filter_col_names` and exclude it from the SELECT. Guard with `other.value is not None`.

```python
filter_col_names = set()
for cond in item.conditions:   # ← item.conditions, not conditions
    if isinstance(cond, BinaryOperation) and len(cond.args) >= 2:
        for i, arg in enumerate(cond.args[:2]):
            if isinstance(arg, Identifier):
                other = cond.args[1 - i]
                if isinstance(other, Constant) and other.value is not None:  # ← non-null only
                    filter_col_names.add(arg.parts[-1])
fetch_cols = referenced_cols - filter_col_names
```

---

## Handler Checklist

When creating or modifying a handler's `select()` method:

- [ ] Does the handler use target columns to control API parameters (Pattern B)?
  - If yes: use `_collect_identifiers()` to recursively extract column names.
- [ ] Does the handler fetch all data and then filter by column (Pattern A)?
  - If yes: add `else: selected_columns = self.get_columns(); break` and a `if not selected_columns: selected_columns = self.get_columns()` guard.
- [ ] Never `raise ValueError` on unrecognised target types — complex expressions are valid inputs from the planner.
- [ ] Never leave `selected_columns` empty after the targets loop — that silently drops all result columns.
- [ ] WHERE filter params (e.g., `start_date`, `end_date`) should be extracted from `query.where` and passed to the API, not treated as SELECT dimensions.
- [ ] `get_columns()` must list every column the API can return so Pattern A drop-logic works correctly.

---

## Handlers in This Project

| Handler | Pattern | Notes |
|---|---|---|
| `google_analytics_handler` | B | Uses `_collect_identifiers`; target columns map to GA4 dimensions/metrics |
| `google_calendar_handler` | A | Fetches all events/calendars/free-busy, then filters columns |
| `google_search_handler` | A | Fetches traffic/sitemaps/url-inspection data, then filters columns |
| `email_handler` | A (via `SELECTQueryParser`) | Delegated to utility — safe |
| `hubspot_handler` | A (via `SELECTQueryParser`) | Delegated to utility — safe |
| `shopify_handler` | A (via `SELECTQueryParser`) | Delegated to utility — safe |
| `xero_handler` | A | No target iteration — safe |
| `ms_one_drive_handler` | A | String checks only — safe |
| `web_handler` (`url_reader`) | A | Uses `FilterCondition`, no target iteration — safe |
| `s3_handler` | A | Only scans targets for `"content"` key; full query passed to DuckDB |

---

## Relevant Source Paths

| File | Purpose |
|---|---|
| `mindsdb/api/executor/planner/query_planner.py` | `plan_select`, `plan_cte`, `plan_api_db_select`, `get_integration_select_step` |
| `mindsdb/api/executor/planner/plan_join.py` | `PlanJoinTablesQuery`, `process_table`, `get_filters_from_join_conditions` |
| `mindsdb/api/executor/sql_query/steps/subselect_step.py` | `SubSelectStepCall` — runs DuckDB on handler result |
| `mindsdb/api/executor/utilities/sql.py` | `query_df`, `query_df_with_type_infer_fallback` |
| `mindsdb/integrations/utilities/query_traversal.py` | `query_traversal` — AST walker used across planner and handlers |
| `mindsdb/integrations/handlers/<name>/` | Individual handler implementations |

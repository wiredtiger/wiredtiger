# Codebase Lookup

Subagent path for source lookup and prior-ticket search. Called from @paths/investigate.md Step 7.

## Inputs (from investigate.md Step 3)

- Assertion text
- Function name (or "unknown")
- file:line from stack trace (or "unavailable")

## Subagent

```
Agent(
  subagent_type="general-purpose",
  prompt="""
Codebase lookup for a WiredTiger BF. Return a structured summary only — no raw grep dumps.

Inputs:
- Assertion text: <text>
- Function name: <name, or "unknown">
- file:line from stack: <file:line, or "unavailable">

Tasks:
1. Find the assertion or function in source:
     grep -rn "<assertion text>" src/
     grep -rn "<function_name>" src/ --include="*.c" --include="*.h"
   Record file:line. Quote the assertion and the 5 lines above it verbatim.

2. Identify the subsystem from the file path/prefix:
   - __wt_page_*, __wt_btree_*, src/btree/       → B-tree / cursor
   - __wt_txn_*, src/txn/                         → Transaction / timestamp
   - __wt_evict_*, src/evict/                     → Eviction / cache
   - __wt_ckpt_*, src/checkpoint/                 → Checkpoint
   - __wt_log_*, src/log/                         → Durability / logging
   - __wt_rts_*, src/rollback_to_stable/          → Rollback-to-stable
   - src/block_disagg/, src/tiered/               → Disaggregated / tiered storage
   - test/suite/ Python AssertionError            → API or functional test
   If none match: "unknown."

3. Search Jira for prior tickets on this assertion:
     mcp__devprod-mcp-gateway__jira_search_issues(
       jql="project = WT AND text ~ \"<assertion text>\" ORDER BY created DESC"
     )
   List any results: ticket key, status, whether a fix commit is mentioned.

Return only:
- Assertion location: file:line
- 5 lines above the assertion (verbatim)
- Subsystem: <name or "unknown">
- Prior tickets: <list with status, or "none found">
"""
)
```

## Returns to investigate.md

- **Assertion location:** `<file:line, or "unavailable">`
- **5 lines above:** `<verbatim>`
- **Subsystem:** `<name, or "unknown">`
- **Prior tickets:** `<list with status, or "none found">`

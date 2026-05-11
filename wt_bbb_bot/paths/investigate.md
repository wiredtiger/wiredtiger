# Investigate Path

Use this path for a deep-dive into a specific WiredTiger BF — crash, assertion, hang,
data corruption, or stubborn intermittent.

## When to enter this path

- Initial triage (SKILL.md Step 4) classified the failure as crash, assertion, hang, or corruption
- The log summary was not enough to form a hypothesis
- A root cause hypothesis exists but needs verification

## Tools

- `evg_get_raw_task_logs` — full task logs (use `log_type=task` first, then `system`)
- `evg_get_test_results_detailed` — raw test output with error patterns
- `jira_get_issue`, `jira_get_issue_comments` — full BF history
- `bb_get_bfg` — failure group timeline; is this a recent regression or long-standing?
- `jira_search_issues` — search for related SERVER/WT tickets with JQL

## Investigation workflow

### Step 1: Extract the exact failure signature
From the raw logs, find:
- The **first** error or assertion (not a cascade effect)
- Full stack trace — note file, function, and line
- Any preceding log lines that show what WT was doing (checkpoint, eviction, txn commit, etc.)
- Relevant config: `wiredtiger_open` string, test flags, storage config

### Step 2: Search for prior art
```
jira_search_issues: project in (BF, WT) AND text ~ "<assertion text or function name>" ORDER BY created DESC
```
Also use `mcp__claude_ai_Glean_via_MCP__search` for internal Confluence/Slack context on
the failing function or subsystem.

### Step 3: Map the failure to WT subsystem

| Signal | Likely subsystem |
|---|---|
| `__wt_page_*`, `__wt_btree_*` | B-tree / cursor |
| `__wt_txn_*`, `WT_TXN_*` | Transaction / timestamp |
| `__wt_evict_*`, `WT_EVICT_*` | Eviction / cache |
| `__wt_ckpt_*`, `__wt_checkpoint` | Checkpoint |
| `__wt_log_*` | Durability / logging |
| `__wt_rts_*` | Rollback-to-stable |
| `block_disagg`, `tiered` | Disaggregated / tiered storage |
| Python `AssertionError` in `test/suite/` | API or functional test |

### Step 4: Verify the hypothesis
Before concluding, confirm:
- The commit window: is the failure new (regression) or long-standing?
- Check `bb_get_bfg` — when did failures start? Any gap in history?
- If the stack points to a specific commit, check git blame in the WT repo

For local verification: see @repro-format.md to reproduce, or @wt-cli.md to inspect data.

## Output format

### Current understanding
One paragraph.

### Evidence
- **Source:** (EVG task ID / BF ID / Jira key)
- **First error:** (exact line)
- **Stack trace summary:** (file:line → file:line chain)
- **WT state at failure:** (what was the engine doing?)
- **Prior occurrences:** (related BFs or tickets)

### Root cause hypothesis
One paragraph — the most likely explanation and why.

### Confidence
Low / Medium / High — and the key uncertainty.

### Next checks
1. ...
2. ...

# Investigate Path

Use this path for:
- failing WT tests
- intermittent behavior
- regressions with unclear cause
- "why did this happen?"
- "what should I check first?"
- crash, assertion, hang, or data corruption requiring deep-dive

## When to enter this path

- Failure cause is unclear — start at Step 1
- Initial triage (SKILL.md Step 4) classified the failure — start at Step 2
- A root cause hypothesis exists but needs verification — start at Step 3

## Tools

- `evg_get_raw_task_logs` — full task logs (use `log_type=task` first, then `system`)
- `evg_get_test_results_detailed` — raw test output with error patterns
- `jira_get_issue`, `jira_get_issue_comments` — full BF history
- `bb_get_bfg` — failure group timeline; is this a recent regression or long-standing?
- `jira_search_issues` — search for related SERVER/WT tickets with JQL
- `mcp__claude_ai_Glean_via_MCP__search` — internal Confluence/Slack context

---

## Step 1: Gather BF-specific evidence

Before applying any debugging methodology, collect the WT-specific context:

**From Jira + Build Baron:**
- Fetch the BF ticket — linked EVG tasks, CAUSES relationships, sibling BFs
- Check `bb_get_bfg` — when did failures start? Is this a recent regression or long-standing?
- Read prior comments — is someone already investigating?

**From Evergreen logs:**
- `evg_get_task_log_summary` for a quick scan, then `evg_get_raw_task_logs` for the full log
- Extract: first error line, stack trace, test name, and what WT was doing at failure time
- Extract: `wiredtiger_open` config string, test flags, build variant

**Classify the failure type:**

| Type | Characteristics | Next |
|---|---|---|
| Crash / SIGABRT | Stack trace with signal or `wiredtiger_abort` | Step 2 |
| Assertion failure | `WT_ASSERT`, `__wt_errx`, or Python `AssertionError` | Step 2 |
| Hang / timeout | Task timeout, no progress in logs | Step 2 |
| Data corruption | `verify` failure, unexpected key/value | Step 2 |
| Flaky / intermittent | Passes sometimes, low failure rate | @build.md to measure rate |
| Environment / infra | OOM, disk full, network, agent crash | Note and close as infra |

---

## Step 2: Map to WT subsystem

Use the stack trace or failing function to identify the subsystem:

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

Search for prior art:
```
jira_search_issues: project in (BF, WT) AND text ~ "<assertion text or function name>" ORDER BY created DESC
```

Also search Glean for internal Confluence/Slack context on the failing subsystem.

---

## Step 3: Apply systematic debugging methodology

With the WT-specific evidence gathered, apply the full investigation process:

→ **@skills/systematic-debugging/SKILL.md** — root cause investigation, pattern analysis,
hypothesis and testing, implementation

For local reproduction: → **@build.md**
For data directory inspection: → **@wt-cli.md**

---

## Output format

### Current understanding
One short paragraph.

### Evidence gathered
- failure:
- repro:
- recent change:
- logs or artifacts:

### Working theory
One short paragraph — most likely cause and confidence level.

### Next checks
1. ...
2. ...

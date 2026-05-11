# Investigate Path

Use this path for:
- failing WT tests
- intermittent behavior
- regressions with unclear cause
- "why did this happen?"
- "what should I check first?"
- crash, assertion, hang, or data corruption requiring deep-dive

## When to enter this path

- Failure cause is unclear — start at Phase 0
- Initial triage (SKILL.md Step 4) classified the failure — start at Phase 1
- A root cause hypothesis exists but needs verification — start at Phase 2

## Tools

- `evg_get_raw_task_logs` — full task logs (use `log_type=task` first, then `system`)
- `evg_get_test_results_detailed` — raw test output with error patterns
- `jira_get_issue`, `jira_get_issue_comments` — full BF history
- `bb_get_bfg` — failure group timeline; is this a recent regression or long-standing?
- `jira_search_issues` — search for related SERVER/WT tickets with JQL
- `mcp__claude_ai_Glean_via_MCP__search` — internal Confluence/Slack context

---

## Phase 0: Triage (start here when cause is unclear)

### Step 1: Read the full error
- Capture exact error text, assertion, or signal
- Note the test name and file
- Record exact reproduction steps from the Evergreen task log

### Step 2: Check recent changes
- Has this test ever passed? Check `bb_get_bfg` for failure history
- Any recent WT commits touching the failing subsystem?
- Linked SERVER/WT tickets with CAUSES relationships?

### Step 3: Gather evidence at component boundaries
- What was the engine doing? (checkpoint, eviction, txn commit, recovery)
- Any config flags that change behavior? (`wiredtiger_open` string, test flags)
- Is the failure deterministic or intermittent?

### Step 4: Classify

| Type | Characteristics | Next |
|---|---|---|
| Crash / SIGABRT | Stack trace with signal or `wiredtiger_abort` | Phase 1 |
| Assertion failure | `WT_ASSERT`, `__wt_errx`, or Python `AssertionError` | Phase 1 |
| Hang / timeout | Task timeout, no progress in logs | Phase 1 |
| Data corruption | `verify` failure, unexpected key/value | Phase 1 |
| Flaky / intermittent | Passes sometimes, low failure rate | @build.md to measure rate |
| Environment / infra | OOM, disk full, network, agent crash | Note and close as infra |

---

## Phase 1: Root cause investigation

### Step 1: Extract the exact failure signature
From the raw logs, find:
- The **first** error or assertion (not a cascade effect)
- Full stack trace — note file, function, and line
- Any preceding log lines that show what WT was doing
- Relevant config: `wiredtiger_open` string, test flags, storage config

### Step 2: Find a nearby working example
- Search for a similar test that passes
- Compare broken vs working behavior — list concrete differences

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

### Step 4: Search for prior art
```
jira_search_issues: project in (BF, WT) AND text ~ "<assertion text or function name>" ORDER BY created DESC
```

---

## Phase 2: Hypothesis and testing

- Write **one explicit hypothesis** — what broke, where, and why
- Test the **smallest change or check** that can prove or disprove it
- Do not stack multiple speculative fixes
- Confidence: Low / Medium / High — state the key uncertainty

---

## Phase 3: Implementation

Only enter this phase after root cause is understood:
- Create a failing test or reproducer first when possible — see @build.md
- Verify the fix resolves the failure
- Check for fallout in related subsystems

For local reproduction: see @repro-format.md.
For data directory inspection: see @wt-cli.md.

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

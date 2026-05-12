---
name: evergreen
description: Read Evergreen task logs, test results, and patch failure data for WiredTiger CI investigations. Use when triaging BFs, mapping task failures to root causes, or fetching CI context for a Jira ticket. Read-only — does not create patches, restart tasks, or modify CI state.
when_to_use: fetching task logs for a BF, identifying the first error in CI logs, mapping a failing test to a code path, checking failure recurrence on the waterfall
---

# Evergreen Skill

## Preferred Method: devprod MCP Gateway

Use `mcp__devprod-mcp-gateway__evg_*` tools when available. No CLI installation or token required.

### Fetch task log summary

```
mcp__devprod-mcp-gateway__evg_get_task_log_summary(task_id="<id>")
```

Extract: the **first** error or assertion (not a cascade effect), test name and source file referenced, whether the failure is deterministic or intermittent in the log. **Stop here when the summary shows the first error clearly** — most cases don't need full logs.

### Fetch raw task logs (when summary is insufficient)

```
mcp__devprod-mcp-gateway__evg_get_raw_task_logs(task_id="<id>", log_type="task")
```

`log_type` can be `task` (main), `agent` (Evergreen runner), or `system` (host). **Log files can be very large** — start from the bottom (last lines first); errors are usually near the end. Stop after identifying the first error and capture 20 lines before/after for context.

### Get test results

```
mcp__devprod-mcp-gateway__evg_get_test_results_summary(task_id="<id>")    # which tests passed/failed
mcp__devprod-mcp-gateway__evg_get_test_results_detailed(task_id="<id>")   # raw output and error for failing tests
```

For Python suite / catch2 / unit test tasks, these endpoints are cleaner than parsing logs.

### Get patch-level failure context

```
mcp__devprod-mcp-gateway__evg_get_patch_failed_jobs(patch_id="<id>")
mcp__devprod-mcp-gateway__evg_list_user_recent_patches(...)
```

Use when the BF references a patch (not the waterfall) — reveals whether the BF is one of many cascading failures or an isolated regression.

### Discover project identifiers and artifacts

```
mcp__devprod-mcp-gateway__evg_get_inferred_project_ids(...)
mcp__devprod-mcp-gateway__evg_download_task_artifacts(task_id="<id>")
```

Use when only a branch is known (find the project ID) or when core dumps / generated logs need to be downloaded.

---

## Fallback Method: Evergreen CLI

Use only if the MCP gateway is unavailable. Requires the `evergreen` CLI configured via `~/.evergreen.yml` (download from `https://evergreen.mongodb.com/settings`).

### Fetch task info

```bash
evergreen task --task-id <task_id>
```

### Fetch task logs

```bash
# Main task log
evergreen fetch --task <task_id>

# Test results
evergreen task --task-id <task_id> --tests
```

### List patches

```bash
# Recent patches
evergreen list-patches -n 10

# Failed tasks in a specific patch
evergreen patch --patch-id <patch_id>
```

---

## WiredTiger context

### Evergreen projects

| Project | What runs there |
|---|---|
| `wiredtiger-mongo-master` | WT-only CI on the `develop` branch |
| `wiredtiger-mongo-v8.X` / `v7.0` | WT-only CI on release branches |
| `mongodb-mongo-master` | Full MongoDB CI — WT BFs surface here too |
| `mongodb-mongo-v8.X` / `v7.0` | Full MongoDB release-branch CI |

A single BF can come from either the WT-only project or the MongoDB project — the `Evergreen Project` custom field on the BF ticket says which.

### Read the BF ticket first

The BF ticket already carries everything you need from Evergreen — read these Jira custom fields **before** calling Evergreen tools:

| BF custom field | Use it as |
|---|---|
| `Failing Tasks` | Direct list of failing task names |
| `Failing Buildvariants` | The variant(s) that produced the failure |
| `Evergreen Project` | The project(s) the failures came from |
| `First Failing Revision` | Commit SHA that broke it — feed to git blame |

If you don't have a task **ID** (just a name + variant + project), the BF description usually links to the Evergreen task — extract the task ID from the URL (it's the long hex string in the path).

### Failure classification

→ **@paths/triage.md Step 4** for the canonical failure type table and routing.

Log signatures to match against before classifying:
- Crash: `wiredtiger_abort`, signal/segv, `core dump`, gdb stack trace
- Assertion: `WT_ASSERT`, `__wt_assert`, `__wt_errx`, `__wt_panic`
- Python assertion: `testtools.testresult.real._StringException`, traceback through `wttest.py`
- Hang: task timeout, no progress, stuck on `wt_open` or checkpoint
- Corruption: `wt verify` failure, unexpected key/value, `WT_VERB_LOG NOTICE: record len corruption`
- Compilation: `archive_dist_test` failure with compiler errors — fix first, cascades to test failures
- Lint: `s_all` / `s_fast` violations — quick fix, independent

### WT log-prefix → subsystem map

These prefixes in raw logs map directly to WT subsystems — use them to route investigation:

| Log prefix / function | Subsystem |
|---|---|
| `__wt_page_*`, `__wt_btree_*` | B-tree / cursor |
| `__wt_txn_*`, `WT_TXN_*`, `WT_VERB_TXN` | Transaction / timestamp |
| `__wt_evict_*`, `WT_EVICT_*`, `WT_VERB_EVICT` | Eviction / cache |
| `__wt_ckpt_*`, `__wt_checkpoint`, `WT_VERB_CHECKPOINT` | Checkpoint |
| `__wt_log_*`, `WT_VERB_LOG`, `WiredTigerLog.000*` | Durability / logging |
| `__wt_rts_*`, `WT_VERB_RTS` | Rollback-to-stable |
| `block_disagg`, `tiered`, `WT_VERB_DISAGG` | Disaggregated / tiered storage |

The Python suite test name itself often points at the subsystem (`test_checkpoint*`, `test_rollback_to_stable*`, `test_disagg*`, etc.).

### Antithesis BFs are special

If the BF has the `antithesis` label or `Failing Buildvariants` includes `libvoidstar`:

- The "reproducer" is **not** a local `test/format` run — it's a multiverse-debugger session at `antithesis.com`.
- The BF description usually contains the antithesis session ID, input hash, and vtime needed to replay.
- These are often **rare-event crashes** with narrow trigger windows — check the BF's `De-escalation Justification` field before recommending action.

### Exclusions — do NOT recommend a fix

- **Master / develop branch failures** unrelated to a specific BF — wait for upstream resolution; the maintainer of `develop` is on it.
- **Known flaky tests** — if the same test fails intermittently across unrelated commits, mark and move on.
- **Host allocation / archive / agent crashes** — CI infrastructure issues; escalate to the Build team.
- **Network timeouts during fetch** — transient infra; suggest re-run, not code fix.

### Deduplication

- A single root cause often produces many task failures across variants — group them when reporting.
- **Compilation failures cascade** into test failures across the same patch — fix compile first, re-evaluate test results after.
- For a BF that lists multiple `Failing Tasks` or `Failing Buildvariants`, the first task you investigate usually explains the rest.

### Standard output structure

After running this skill, return to the super-agent:

#### First error
Exact line, with `file:line` if visible.

#### Stack trace summary
Top 3–5 frames.

#### Failure classification
One of: crash / assertion / hang / corruption / flaky / compilation / lint / infra.

#### Affected subsystem
From the log-prefix map above.

#### Affected variants and project
From the BF `Failing Buildvariants` and `Evergreen Project` fields.

#### First failing revision
The commit SHA (from the BF custom field).

#### Antithesis?
Yes / No.

#### Recommendation
Which skill or path to invoke next — investigate / build / wt-cli / close as infra.

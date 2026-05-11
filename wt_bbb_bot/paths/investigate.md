# Investigate Path

Autonomous investigation of a WiredTiger ticket. Input is a WT ticket key. The bot
executes all steps without waiting for user input, posts its findings to Jira, and
terminates with a single resolved next action.

**Core rule:** Only assert what you fetched. Unknown = record as "unknown." Never fill
a gap with an assumption. When in doubt, do less and say more.

---

## Step 1: Jira Context

→ **@skills/jira/SKILL.md** — fetch ticket and comments:

```
mcp__devprod-mcp-gateway__jira_get_issue(issue_key="WT-XXXXX")
mcp__devprod-mcp-gateway__jira_get_issue_comments(issue_key="WT-XXXXX")
```

Extract and carry forward through all remaining steps:

| Field | Extract |
|---|---|
| Summary | Title of the failure |
| Status / Priority / Assignee | Or "unowned" |
| EVG task IDs | All URLs or IDs in the description |
| CAUSES / IS CAUSED BY links | Any linked WT or SERVER tickets |
| Last comment date | |
| Prior investigation in comments | yes / no / partial — one-line summary |

**Decision: is the root cause already known?**

If a CAUSES link points to a closed ticket with a fix commit → skip to Output, mark
confidence High, recommended action: verify the fix is merged to the affected branch.

If a prior comment from the last 48 hours shows an active investigation → skip to
Output, record what was found, recommended action: no duplicate work needed.

Otherwise → continue to Step 2.

---

## Step 2: Occurrence Analysis

### 2a: Fetch the failure group

```
bb_get_bfg_by_task(task_id)          # preferred
bb_search_bfgs(query)                # fallback if task_id unavailable
bb_get_bfg(bfg_id)                   # fetch full group details once you have the ID
```

Extract:

| Field | Extract |
|---|---|
| Failure group ID | |
| Last-good SHA / First-bad SHA | From Build Baron — carry into Step 4 |
| Total failures (all time) | N |
| Time window | First seen → last seen |
| Failures in last 7 days | N |
| Distinct variants affected | List — platform, sanitizer, build type |
| Blocking trunk or a release? | yes / no / unknown |

### 2b: Characterize the variant pattern

Record which variants are failing vs. not failing as observed facts:
- Platform spread: all platforms / Linux only / macOS only / Windows only
- Sanitizer spread: all build types / sanitizer only (specify which) / non-sanitizer only
- Build type: debug / release / specific variant name

### 2c: Establish the timeline

From the failure group:
- Date of first failure
- Failure rate trend: increasing / stable / declining / single burst
- If burst: time window of all failures (e.g. "all 6 within 3 hours on 2026-05-07")

### 2d: Search for sibling failures

→ **@skills/jira/SKILL.md**:

```
mcp__devprod-mcp-gateway__jira_search_issues(
  jql="project = WT AND text ~ \"<test name>\" AND created >= -14d ORDER BY created DESC"
)
```

If siblings exist with CAUSES links or investigation comments, read them and carry
the findings forward. They may resolve the investigation here.

### 2e: Occurrence summary

State numbers only — no interpretation:

- **Total failures:** N
- **Time window:** first seen → last seen
- **Failures in last 7 days:** N
- **Variants affected:** list
- **Failure pattern:** evenly spread / burst / single occurrence
- **Siblings found:** yes (list ticket keys) / no

---

## Step 3: Evergreen Logs

Escalate through these calls in order — stop at the first one that yields the first
error line and stack trace:

**3a — Log summary:**
```
evg_get_task_log_summary(task_id)
```

**3b — Raw task log (if 3a is insufficient):**
```
evg_get_raw_task_logs(task_id, log_type="task")
```

**3c — System log (if the process died silently):**
```
evg_get_raw_task_logs(task_id, log_type="system")
```

**3d — Test results (if per-test detail is needed):**
```
evg_get_test_results_summary(task_id)
evg_get_test_results_detailed(task_id)
```

When scanning a large raw log, search for these signals first:
`WT_PANIC`, `wiredtiger_abort`, `SIGABRT`, `WT_ASSERT`, `AssertionError`, `Assertion failed`

Extract — quote log lines exactly, never paraphrase:

| Field | Extract |
|---|---|
| Test name / file | |
| First error line | Exact quoted string |
| Stack trace | Top 5 frames, or "unavailable" |
| `wiredtiger_open` config | From log, or "not found" |
| Build variant / CMake flags | |
| Failure appears deterministic? | yes / no / unclear from log |
| 20 lines before first error | Paste verbatim |

---

## Step 4: Git History

→ **@skills/github/SKILL.md** — use the last-good/first-bad SHAs from Step 2a and the
failing function or assertion text from Step 3.

**4a — Commits touching the relevant subsystem between good and bad builds:**

```
mcp__devprod-mcp-gateway__git_log(
  from="<last_good_sha>",
  to="<first_bad_sha>",
  path="src/<subsystem>/"
)
```

If the subsystem is not yet known, omit `path` and scan the full window, then filter
manually for commits touching files related to the failure signal.

**4b — Blame the exact line from the stack trace:**

When Step 3 gave a `file:line`, find when that line last changed:

```
mcp__devprod-mcp-gateway__git_blame(file="src/<path>.c", line=<N>)
mcp__devprod-mcp-gateway__git_show(sha="<sha from blame>")
```

Look at the diff for: does the change touch the assertion or invariant that failed?

**4c — Search commits by assertion text or ticket key:**

```
mcp__devprod-mcp-gateway__git_search(query="<assertion text or WT-XXXXX>")
```

Useful when the assertion text is distinctive or you have a suspect ticket from
sibling BF comments.

**4d — Diff between good and bad builds (if window is small):**

```
mcp__devprod-mcp-gateway__git_diff(from="<last_good_sha>", to="<first_bad_sha>")
```

Use only when the window contains few commits — otherwise the output is too large to
reason about. Filter to the subsystem path if possible.

**Record for each candidate commit:**

| Field | Extract |
|---|---|
| Commit SHA | |
| Author / date | |
| Summary | one-line message |
| Files changed | relevant to the failing subsystem? yes / no |
| Linked ticket | WT-XXXXX or SERVER-XXXXX from commit message |

**Decision:** If a commit lands within the failure window and touches the failing file
or function, flag it as a suspect. State it as a fact:
- "Commit `abc1234` (WT-XXXXX, 2026-05-07) modified `src/txn/txn.c` one day before
  the first failure."

Do not conclude it caused the bug — that belongs in the working theory.
If no candidate commit is found, record "no suspect commit identified."

---

## Step 5: Codebase Lookup

Skip this step if Step 3 produced no concrete signal (no assertion text, no function
name, no `file:line`). Record as unknown and proceed to Output.

**5a — Find the assertion or function in source:**

```bash
grep -rn "<assertion text>" src/
grep -rn "<function_name>" src/ --include="*.c" --include="*.h"
```

Record the file path and line number. Quote the assertion and the 5 lines above it
verbatim — do not interpret intent.

**5b — Identify the subsystem:**

| Prefix / path | Subsystem |
|---|---|
| `__wt_page_*`, `__wt_btree_*`, `src/btree/` | B-tree / cursor |
| `__wt_txn_*`, `src/txn/` | Transaction / timestamp |
| `__wt_evict_*`, `src/evict/` | Eviction / cache |
| `__wt_ckpt_*`, `src/checkpoint/` | Checkpoint |
| `__wt_log_*`, `src/log/` | Durability / logging |
| `__wt_rts_*`, `src/rollback_to_stable/` | Rollback-to-stable |
| `src/block_disagg/`, `src/tiered/` | Disaggregated / tiered storage |
| `test/suite/` Python `AssertionError` | API or functional test |

If none match: subsystem = "unknown."

**5c — Search for prior tickets:**

→ **@skills/jira/SKILL.md**:

```
mcp__devprod-mcp-gateway__jira_search_issues(
  jql="project = WT AND text ~ \"<assertion text>\" ORDER BY created DESC"
)
```

Record any prior tickets, their status, and whether a fix commit exists.

---

## Output

Populate every field. Write "unknown" or "insufficient data" rather than omitting a
field. This step is read-only — do not post to Jira or modify any external state.

---

### Jira context
- **Ticket:** `WT-XXXXX` — `<summary>`
- **Status / Priority / Assignee:** `<values>`
- **CAUSES links:** `<list, or none>`
- **Prior investigation:** `<yes — summary | no | partial — summary>`

### Occurrence analysis
- **Total failures:** N over X days across Y variants
- **Failure pattern:** `<evenly spread / burst / single occurrence>`
- **Variants:** `<list>`
- **Blocking trunk / release:** `<yes / no / unknown>`
- **Siblings:** `<list, or none>`

### Log evidence
- **Test:** `<name and file>`
- **First error (exact):** `"<quoted line>"`
- **Stack trace:** `<top frames, or "unavailable">`
- **`wiredtiger_open` config:** `<value, or "not found in log">`
- **Build variant:** `<value, or unknown>`

### Git history
- **Suspect commit:** `<SHA — author, date, summary, or "none identified">`
- **Linked ticket:** `<WT-XXXXX or SERVER-XXXXX from commit message, or none>`
- **In failure window:** `<yes / no / unverified>`

### Codebase
- **Subsystem:** `<name, or unknown>`
- **Assertion location:** `<file:line, or unavailable>`
- **Prior tickets:** `<list with status, or "none found">`

### Unknowns
- ...

### Working theory
*Only write this if log and code evidence directly support it. Otherwise write:
"Insufficient evidence — see unknowns."*

**Confidence:** Low / Medium / High
**What would confirm or refute this:** `<one specific thing>`

### Next action

Pick exactly one:

- **Needs local repro** — failure rate and evidence support reproduction: `@paths/build.md`
- **Needs data inspection** — failure points to persisted state or corruption: `@skills/wt-cli/SKILL.md`
- **Needs disagg inspection** — failure is in SLS / block_disagg: `@skills/disagg-page-inspection/SKILL.md`
- **Needs owner** — assign to `<team>` because `<reason>`
- **Infra issue** — evidence: `<log lines showing OOM / disk / agent crash>`
- **Already fixed** — by `<WT-XXXXX or commit SHA>`; verify merge to affected branch
- **Insufficient data** — `<what is missing and how to get it>`

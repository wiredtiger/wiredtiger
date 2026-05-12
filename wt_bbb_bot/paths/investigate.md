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
| CI blocker? | yes / no / unknown |

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

**Intermittency assessment — carry this forward into all repro and fix-verification steps:**

CI recurrence does not prove local reproducibility. Always treat failures as potentially intermittent regardless of BFG count. Use BFG count only to set a floor on iterations:

| BFG count (30 days) | Min iterations for repro/verification |
|---|---|
| ≥ 5 | 10 |
| 2–4 | 20 |
| 1 (single occurrence) | 30 |

A single passing local run is not evidence of anything. Never claim "not reproduced" or "already fixed" without meeting the minimum iteration count.

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

**Critical rule — the Jira description is never sufficient as a log source:**
The error text in a Jira ticket description is truncated and may omit the full WiredTiger
error string (e.g., "commit timestamp 0x1 must be after stable timestamp 0x3a2f"). Always
fetch at least 3a before accepting any error as understood. If 3a is unavailable (401,
missing task ID), note the skip explicitly and record "log evidence: unavailable" — do NOT
treat the Jira excerpt as equivalent to reading the log.

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

## Step 6: Reproduction Attempt

Attempt reproduction if **all** of the following hold:
1. Step 3 yielded a concrete error signal (assertion text, test name, or test command).
2. Step 5 identified the subsystem (not "unknown").
3. The failure is not already explained by a known fix commit (which would make repro unnecessary).

If any condition is not met, skip this step and record "Reproduction: skipped — `<reason>`" in the Output.

**Spawn a sub-agent to run the repro** — do not run it inline. This keeps noisy build/test output out of the investigation context.

Prompt the sub-agent with:
- The ticket key and one-line failure summary.
- The exact test name and command from Step 3.
- The build variant from Step 3 (e.g. ASan, debug, release).
- The suspect commit / code location from Steps 4–5 (for context only — the sub-agent does not fix anything).
- Instruction to follow `@paths/build.md` — sections "Reproducing a BF Failure" Steps 1–4 only. No fix proposal, no code changes.
- The minimum iteration count from Step 2c's intermittency table.

**The sub-agent must:**
1. Build the matching variant if the build directory does not exist.
2. Run the test at the minimum iteration count.
3. Return the reproduction output block from `@paths/build.md` (mode, command, config, build variant, workers, result, failure rate, first failure log path).

**Record the result verbatim in the Output.** Do not interpret "no failure in N runs" as "fixed" unless the minimum iteration count was met. If the sub-agent fails to build or returns an error, record that as the result.

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
- **CI blocker:** `<yes / no / unknown>`
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

### Reproduction
- **Result:** `reproduced | not reproduced | inconclusive | skipped — <reason>`
- **Command:** `<exact command run>`
- **Build variant:** `<value>`
- **Iterations run:** N
- **Failure rate:** X/N
- **First failure log:** `<path, or "n/a">`

### Unknowns
- ...

**Before writing any field as "unknown" or choosing "Insufficient data" as the next action,
confirm you have exhausted these sources in order:**

| Source | Done? | Notes |
|---|---|---|
| Jira description + comments | | |
| Evergreen log (3a summary or 3b raw) | | |
| Build Baron failure group (Step 2) | | |
| Sibling BF tickets (Step 2d) | | |
| Source code for the failing function / assertion (Step 5) | | |
| Git history in the failure window (Step 4) | | |
| Local build + test run to observe actual error output | | |

Only after ticking all applicable rows may a field be recorded as "unknown". If a source
is unavailable (e.g., 401 on Evergreen, macOS-only failure), record *why* it is
unavailable — "unknown" alone is not an acceptable entry when a source was never attempted.

**Local build rule:** You MUST attempt local reproduction before writing "unknown".
This is not optional. Steps in order:

1. Build WiredTiger (`cd build && ninja` — incremental if build dir exists).
2. Run the exact failing test: `python3 ../test/suite/run.py <test> -j1`
   with any hooks the CI used (e.g. `--hook timestamp`).
3. If the test doesn't fail on the first run, run more iterations up to the
   minimum count from Step 2c. Use `--repeat N` or a loop.
4. If the failure requires a platform that cannot be reproduced locally
   (e.g., macOS-only), write a *targeted* reproducer instead — a minimal
   Python script that directly exercises the hypothesis (e.g., sets
   stable_timestamp to a high value and attempts a low-timestamp commit).
   Run that script and record the exact output.
5. Only after steps 1–4 are exhausted may a field be written as "unknown",
   and only with an explicit record of what was run, how many iterations,
   and what the output was.


### Working theory
*Only write this if log and code evidence directly support it. Otherwise write:
"Insufficient evidence — see unknowns."*

**Confidence:** Low / Medium / High
**What would confirm or refute this:** `<one specific thing>`

### Recommended fix

**Rule:** Only write this section if confidence is Medium or High AND you have read the
relevant source file(s) in this session. "Read" means you fetched the actual lines via
Bash grep, Read, or an Explore agent — not that you inferred the content from logs or
prior knowledge.

If you have not read the code:
- Write: `"Not proposed — relevant source not read. Read <file> before proposing a fix."`
- Do NOT write a vague directive like "fix the timestamp lifecycle bug in hook_timestamp.py".

If you have read the code, write:
- **File:** `<path:line range>`
- **Change:** one or two sentences — what specifically changes and why it fixes the violated invariant
- **Risk:** one clause on regression risk

**Fix confidence is capped by reproduction:**

Source analysis alone — tracing the call path, identifying the missing guard, reading
the assertion — justifies a root cause hypothesis. It does not verify the fix.

**Preferred form:** re-run the original failing test. **Fallback:** targeted test or reproducer
(when the original test is not achievable due to platform, timing, or environment).

| Reproduction status | Max fix confidence |
|---|---|
| Original failing test reproduced AND fix verified (failure gone after patch) | Very High |
| Scenario reproduced via targeted test/reproducer AND fix verified | High |
| Reproduced (any form) but fix not yet applied/tested | Medium |
| Not reproduced (timing, seeds, env) — fix proposed from source only | Medium |
| Cannot reproduce at all | Low |

If you propose a fix without a verified reproducer, say so explicitly:
> "Fix proposed from source analysis. Confidence is Medium until a reproduction confirms
> the fix eliminates the assertion."

Do not report High fix confidence on a change you have not tested against a real failure.

### Next action

Pick exactly one:

- **Reproduced — needs fix** — Step 6 reproduced the failure; root cause is identified: `@paths/build.md` Step 5 (fix proposal) and Step 6 (verification)
- **Reproduced — root cause unclear** — failure reproduced but mechanism not yet understood: continue source investigation before proposing a fix
- **Not reproduced** — Step 6 met the minimum iteration count with zero failures; failure may be environment-specific or already fixed: note iteration count and build variant, flag for CI monitoring
- **Repro skipped — needs local repro** — Step 6 preconditions were not met; manual repro required: `@paths/build.md`
- **Needs data inspection** — failure points to persisted state or corruption: `@skills/wt-cli/SKILL.md`
- **Needs disagg inspection** — failure is in SLS / block_disagg: `@skills/disagg-page-inspection/SKILL.md`
- **Needs owner** — assign to `<team>` because `<reason>`
- **Infra issue** — evidence: `<log lines showing OOM / disk / agent crash>`
- **Already fixed** — only use this when ALL of the following are true: (1) a specific fix commit or ticket is identified by name in the source or git log, AND (2) the fix has been verified by running the test at ≥ minimum iterations for the intermittency classification from Step 2c. A single passing run, or source-code inspection alone, is not sufficient evidence.
- **Insufficient data** — `<what is missing and how to get it>`

# Investigate Path

Full investigation of a WiredTiger ticket — triage through reproduction.

**Core rule:** Only assert what you fetched. Unknown = record as "unknown." Never fill a gap with an assumption. When in doubt, do less and say more.

## Explore agent

Before spawning an Explore agent, check `@reference/codebase.md` for orientation — use it to form a targeted question, then verify with source.

```
Agent(subagent_type="Explore", prompt="<question about WiredTiger source>. Search breadth: <quick|medium|very thorough>.")
```

---

# Triage

## Step 1: Fetch WT ticket

`jira_get_issue` + `jira_get_issue_comments` for the WT key. Extract:

| Field | Extract |
|---|---|
| Summary / Status / Priority / Assignee | |
| EVG task IDs | All URLs or IDs in the description |
| CAUSES / IS CAUSED BY links | Any linked WT or SERVER tickets |
| Last comment date | |
| Prior investigation in comments | yes / no / partial — one-line summary |

**Early exit:** if a CAUSES link points to a closed ticket with a fix commit, or a comment from the last 48h shows an active investigation → skip to Output.

## Step 2: Check linked issues + Build Baron

- Read comments on any sibling tickets from the same task/commit.
- `bb_get_bfg_by_task(task_id)` (fallback: `bb_search_bfgs`, then `bb_get_bfg`).

Extract: failure group ID, last-good SHA, first-bad SHA, total failures, time window, variants affected, CI blocker.

## Step 3: Fetch Evergreen logs

→ **@skills/evergreen/SKILL.md** for the full log-fetching escalation sequence.

Inputs: `Failing Tasks`, `Failing Buildvariants`, `Evergreen Project`, `First Failing Revision` from Jira custom fields. Extract the task ID from the Evergreen URL in the description if not present as a field.

Carry forward: test name, first error line (exact quoted), stack trace top frames, build variant, `wiredtiger_open` config.

## Step 4: Classify failure type

| Type | Characteristics | Next |
|---|---|---|
| Crash / SIGABRT | Stack trace with signal or `wiredtiger_abort` | Continue → Step 5 |
| Assertion failure | `WT_ASSERT`, `__wt_errx`, or Python `AssertionError` | Continue → Step 5 |
| Hang / timeout | Task timeout, no progress in logs | Continue → Step 5 |
| Data corruption | `verify` failure, unexpected key/value | Continue → Step 5 |
| Flaky / intermittent | Passes sometimes, low failure rate | @paths/build.md to measure rate |
| Environment / infra | OOM, disk full, network, agent crash | Close as infra issue |

## Step 5: Recurrence + blast radius

From the Build Baron failure group (Step 2):
- How many distinct variants are affected?
- Failure rate over the last 7 days?
- Is this a CI blocker?

Set **min repro iterations** from BFG count (30 days): ≥ 5 → 10 iterations | 2–4 → 20 | 1 → 30.

---

# Investigation

## Step 6: Git History

→ **@paths/git-history.md** — pass last-good/first-bad SHAs (Step 2), failing file/function and assertion text (Step 3).

Returns: suspect commit list (SHA, date, ticket, reason flagged).

## Step 7: Codebase Lookup

Skip if Step 3 produced no concrete signal (no assertion text, no function name, no `file:line`). Record "Codebase: skipped — no signal" and proceed to Output.

→ **@paths/codebase-lookup.md** — pass assertion text and function name (Step 3).

Returns: assertion location (file:line), 5 lines above (verbatim), subsystem, prior tickets.

## Step 8: Reproduction

Skip if any of the following hold: Step 3 has no concrete error signal / Step 7 subsystem is "unknown" / failure is already explained by a known fix commit. Record "Reproduction: skipped — `<reason>`".

→ **@paths/reproduction.md** — pass test command and build variant (Step 3), min iterations (Step 5), suspect commit (Step 6).

Returns: result, failure rate, first error line, log path.

---

# Output

Populate every field. Write "unknown" or "insufficient data" rather than omitting a field. Read-only — do not post to Jira or modify any external state.

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

**Before writing any field as "unknown", confirm you have exhausted these sources:**

| Source | Done? | Notes |
|---|---|---|
| Jira description + comments (Step 1) | | |
| Evergreen log (Step 3) | | |
| Build Baron failure group (Step 2) | | |
| Sibling tickets (Step 2) | | |
| Source code for failing function / assertion (Step 7) | | |
| Git history in failure window (Step 6) | | |
| Local build + test run (Step 8) | | |

### Working theory
*Only write this if log and code evidence directly support it. Otherwise write: "Insufficient evidence — see unknowns."*

**Confidence:** Low / Medium / High / Very High
**What would confirm or refute this:** `<one specific thing>`

### Recommended fix

Only write this section if confidence is Medium or High AND you have read the relevant source file(s). If you have not:
- Write: `"Not proposed — relevant source not read. Read <file> before proposing a fix."`

If you have read the code:
- **File:** `<path:line range>`
- **Change:** one or two sentences — what specifically changes and why
- **Risk:** one clause on regression risk

**Fix confidence cap:**

| Reproduction status | Max fix confidence |
|---|---|
| Original test reproduced AND fix verified | Very High |
| Targeted reproducer AND fix verified | High |
| Reproduced but fix not yet tested | Medium |
| Fix proposed from source only | Medium |
| Cannot reproduce | Low |

### Next action

Pick exactly one:

- **Reproduced — needs fix** — Step 8 reproduced; root cause identified: `@paths/build.md`
- **Reproduced — root cause unclear** — reproduced but mechanism not understood: continue source investigation
- **Not reproduced** — Step 8 met minimum iterations with zero failures: flag for CI monitoring
- **Repro skipped** — Step 8 preconditions not met: `@paths/build.md`
- **Needs data inspection** — failure points to persisted state: `@skills/wt-cli/SKILL.md`
- **Needs owner** — assign to `<team>` because `<reason>`
- **Infra issue** — evidence: `<log lines>`
- **Already fixed** — specific fix commit identified AND fix verified at ≥ min iterations
- **Insufficient data** — `<what is missing and how to get it>`

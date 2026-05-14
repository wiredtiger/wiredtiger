# Investigate Path

Full investigation of a WiredTiger ticket — triage through reproduction.

**Core rule:** Only assert what you fetched. Unknown = record as "unknown." Never fill a gap with an assumption. When in doubt, do less and say more.

**HARD RULE — sub-agents are mandatory:** Every step (0–3) delegates to a path file. Read that file and spawn its `Agent()` block. Never run the step's tools, commands, or lookups inline. Never skip the `Agent()` call and summarize from memory. If a step's `Agent()` cannot be spawned, record it as skipped with the reason and proceed to Output.

---

## Step 0: Triage

→ **@paths/triage.md** — read the file and spawn its `Agent()` block. Pass the ticket key.

Returns: all structured triage fields (Jira context, BFG data, log evidence, failure type, min repro iterations).

---

## Investigation

**Philosophy:** Run every tool. Skip nothing unless it is physically impossible (e.g. no test command exists at all). Partial inputs are not a reason to skip — attempt with what you have and record what came back. Iterate until every cross-feed in the table below has been tried and no tool returns new evidence.

### Tools — all mandatory unless the hard skip condition is met

**Git History** → **@paths/git-history.md**
Finds suspect commits in the failure window.
- Hard skip only if: no SHAs, no dates, no file, no function, and no assertion text — nothing to search on.
- If window is unknown: use a 90-day lookback from the first observed failure date.
- If file/function unknown: search by assertion text string alone.
- Inputs: last-good/first-bad SHAs (or 90-day window), failing file/function, assertion text.
- Returns: suspect commit list (SHA, date, ticket, reason flagged).

**Codebase Lookup** → **@paths/codebase-lookup.md**
Locates the assertion in source, reads the surrounding code, identifies the subsystem.
- Hard skip only if: no assertion text, no function name, no `file:line`, and no unique keywords from the error — nothing to grep for.
- If `file:line` unknown: grep the assertion text string or unique error keywords across the whole source tree.
- Inputs: assertion text, function name, file:line (use whatever subset is available).
- Returns: assertion location (file:line), 5 lines above (verbatim), subsystem, prior tickets.

**Sibling & Prior Ticket Review**
Read every ticket listed in CAUSES links, every sibling from BFG, and every prior ticket returned by Codebase Lookup.
- Hard skip only if: zero related tickets exist anywhere.
- For each ticket: read the description, resolution, and any fix commits linked.
- Returns: prior fix pattern     (if any), whether this is a regression of a known issue.

**Reproduction** → **@paths/reproduction.md**
Attempts local reproduction.
- Hard skip only if: no test command can be determined at all.
- Do not skip because subsystem is unknown or signal is weak — a reproduction result (pass or fail) is itself evidence.
- If failure is already explained by a suspect commit: still reproduce to confirm, then record "Already fixed — confirmed at N iterations."
- Inputs: test command, build variant, min iterations, suspect commit (context only).
- Returns: result, failure rate, first error line, log path.

### Cross-feeds — re-invoke when new signal arrives

Every row below is mandatory when the trigger condition is met. Each re-invocation must add at least one new row to the Evidence Ledger. Stop only when no row below triggers.

| Trigger | Re-invoke | What to pass |
|---|---|---|
| Git History finds a suspect commit | Codebase Lookup | Changed files + functions from that commit |
| Codebase Lookup finds `file:line` | Git History | Narrow `git log -p` to that specific file |
| Codebase Lookup returns a prior ticket | Sibling & Prior Ticket Review | Read that ticket's fix commits and resolution |
| Reproduction gives a stack trace different from triage | Codebase Lookup | New top frame as the assertion text |
| Reproduction gives a stack trace different from triage | Git History | New file:function as the failing location |
| Reproduction passes on HEAD but triage showed failures | Git History | Check for a fix commit between triage date and HEAD |
| Sibling ticket has a fix commit | Git History | Confirm whether that commit is in the failure window |
| Sibling ticket has a fix commit | Codebase Lookup | Read the changed lines in that commit |
| Any tool returns a new function name not yet looked up | Codebase Lookup | That function name |
| Any tool returns a new file:line not yet read | Codebase Lookup | That file:line |

**Cross-feed logging — mandatory:** Each time a cross-feed is triggered (Triggered = yes), before re-invoking the tool, append one line to `/tmp/wt_<ticket>_crossfeed_log.md`:
```
TRIGGER: <trigger text from table> | REINVOKED: yes | RESULT: <one-line result>
```
If a trigger does not fire (Triggered = no), append:
```
TRIGGER: <trigger text from table> | REINVOKED: n/a | JUSTIFICATION: <why trigger did not apply>
```
Create the file if it does not exist. This log is read by the audit agent to verify compliance — it is not self-reported.

---

## Step 4: Fix + Verify

**Gate — run this step only if ALL of the following hold:**
1. `reproduction_result = reproduced`
2. Working Theory confidence will be High or Very High (enough evidence to act on)
3. The relevant source file(s) were read in Codebase Lookup

If the gate is not met, skip this step and record "skipped — <which gate failed>" in the Fix + Verify section of the Output.

**Do not skip this step just because the fix feels obvious or small.** The whole point of having a reproducer is to verify, not just propose.

### 4a. Read the exact source at the bug location

Read the full function containing the bug (not just ±5 lines — the whole function). You need to understand:
- What the function is supposed to do
- Exactly what is missing or wrong
- What the fix must preserve (invariants, error handling, caller contract)

Also read any prior fix commit that was reverted or partially applied — the diff is a strong pattern to follow.

### 4b. Write the exact diff

Write the complete code change. Not a description — the actual lines to add, remove, or modify. Follow the exact style and pattern of adjacent code (same timestamp-set pattern, same error-check idiom, same variable names the function already uses).

If a prior fix commit exists: match its pattern exactly for the unfixed call site. Do not invent a new approach when a validated one exists nearby.

### 4c. Apply the fix

Use the Edit tool to apply the diff to the source file. Do not modify any other file.

### 4d. Rebuild

Use the build commands from @paths/build.md. Build only the target that changed — if the fix is in `test/format/`, rebuild only `t`:
```bash
cd /data/bbb-bot/wiredtiger/build && ninja t
```

If the build fails: record the error, revert the change, mark Step 4 result as "build failed."

### 4e. Verify — re-run the reproducer

Run the same CONFIG and command used in the successful reproduction, using the synchronous `& wait` pattern from @paths/reproduction.md.

Use fresh RUNDIRs (e.g. `RUNDIR_fix_0` through `RUNDIR_fix_3`). Set Bash timeout to `(runs.timer + 4) * 60 * 1000` ms.

Run at least as many instances as the original reproduction (minimum 4). If the original failure rate was low (1/4), run 8 instances to get a stronger signal.

Record:
- Instances run
- Failures seen (with first error line if any)
- Pass rate

**Verification result:**
- `verified` — zero failures across all instances (minimum 4)
- `not verified` — failures still occur with the fix applied
- `inconclusive` — no failures but fewer instances than needed for confidence

---

# Output

Populate every field. Write "unknown" or "insufficient data" rather than omitting a field. Read-only — do not post to Jira or modify any external state.

---

### Jira context
- **Ticket:** `WT-XXXXX` — `<summary>`
- **Status / Priority / Assignee:** `<values>`
- **CAUSES links:** `<list, or none>`
- **Prior investigation:** `<yes — summary | no | partial — summary>`

### Structured fields
- **ci_blocker:** `yes / no / unknown`
- **variants:** N
- **total_failures:** N
- **failures_last_7d:** N
- **suspect_commit:** `yes / no`
- **reproduction_result:** `reproduced / not reproduced / inconclusive / skipped`
- **working_theory_confidence:** `Very High / High / Medium / Insufficient evidence`

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

---

### Evidence ledger

**Complete this before writing Working Theory or Recommended Fix. List only facts that were directly observed — quoted log lines, exact file:line assertions, specific commit SHAs, reproduction counts. No inferences here.**

| # | Fact | Source | Step |
|---|------|--------|------|
| E1 | `<exact quoted text or data>` | `<log URL / file:line / commit SHA>` | `<0/1/2/3>` |
| E2 | ... | ... | ... |

If fewer than two evidence items exist, write:
> **Working Theory: Insufficient evidence — see Unknowns.**
> Do not write a Working Theory or Recommended Fix.

---

### Unknowns

List every gap that blocks narrowing the root cause.

| Gap | Why it blocks progress | How to fill it |
|---|---|---|
| `<what is unknown>` | `<what it would unlock>` | `<concrete next step>` |

**Checklist — confirm each source was exhausted before marking a gap as unresolvable:**

| Source | Checked? | Notes |
|---|---|---|
| Jira description + comments (triage Step 0) | | |
| Evergreen log (triage Step 0) | | |
| Build Baron failure group (triage Step 0) | | |
| Sibling tickets (triage Step 0) | | |
| Source code for failing function / assertion (Step 2) | | |
| Git history in failure window (Step 1) | | |
| Local build + test run (Step 3) | | |

---

### Cross-feed completion

**Gate: fill this table before writing Working Theory. Every triggered row must show a re-invocation result. Mark "not triggered" only if the trigger condition provably did not fire.**

| Trigger | Triggered? | Re-invoked? | Result (one line) |
|---|---|---|---|
| Git History found suspect commit → Codebase Lookup on changed files | yes / no | yes / no / n/a | |
| Codebase Lookup found `file:line` → Git History narrowed to that file | yes / no | yes / no / n/a | |
| Codebase Lookup returned prior ticket → Sibling review | yes / no | yes / no / n/a | |
| Reproduction gave different stack trace → Codebase Lookup on new frame | yes / no | yes / no / n/a | |
| Reproduction gave different stack trace → Git History on new file:function | yes / no | yes / no / n/a | |
| Reproduction passed on HEAD, triage showed failures → Git History for fix commit | yes / no | yes / no / n/a | |
| Sibling ticket had fix commit → Git History confirmed in window | yes / no | yes / no / n/a | |
| Sibling ticket had fix commit → Codebase Lookup on changed lines | yes / no | yes / no / n/a | |
| New function name returned by any tool → Codebase Lookup | yes / no | yes / no / n/a | |
| New `file:line` returned by any tool → Codebase Lookup | yes / no | yes / no / n/a | |

---

### Working theory

**Gate:** Only write this section if the Evidence Ledger contains ≥ 2 items AND every sentence below cites at least one ledger item by number (e.g. [E1]). If the gate is not met, write:
> "Insufficient evidence — see Unknowns."
> and stop. Do not continue to Recommended Fix.

**Prohibited language:** Do not use "likely", "possibly", "may", "might", "could", "suggests", "appears to", "seems", or any other hedging. Every sentence is either a direct inference from cited evidence or it is not written.

State only what the evidence ledger items, taken together, directly establish about the root cause. One paragraph maximum.

**Confidence:** Medium / High / Very High — write your honest assessment; audit will cap if needed.

**What would confirm or refute this:** `<one specific, falsifiable thing>`

---

### Recommended fix

**Gate:** Only write this section if:
1. Working Theory is written (not "Insufficient evidence"), AND
2. The relevant source file(s) were read in Codebase Lookup, AND
3. Reproduction result is `reproduced` or `not reproduced` with a clear code-path reason.

If any gate condition is not met, write:
> `"Not proposed — <which gate failed>."`

If all gates are met:
- **File:** `<path:line range>`
- **Exact diff:** the complete code change (verbatim lines, not a description)
- **Pattern followed:** `<prior fix commit or adjacent code pattern used>`
- **Risk:** one clause on regression risk

---

### Fix + Verify

- **Result:** `verified | not verified | inconclusive | skipped — <reason>`
- **Diff applied:** `<file:line range changed>`
- **Verification command:** `<exact command>`
- **Instances run:** N
- **Failures after fix:** N (first error line if any, or "none")
- **Pass rate:** N/N

---

### Next action

Pick exactly one:

- **Fix verified** — Step 4 applied fix and confirmed zero failures at ≥ min iterations
- **Fix proposed — verification inconclusive** — diff written and applied but not enough iterations to confirm; re-run with more instances
- **Fix proposed — verification failed** — diff applied but failures persist; root cause may be incomplete
- **Reproduced — root cause unclear** — reproduced but mechanism not understood well enough to fix: continue source investigation
- **Not reproduced** — Step 3 met minimum iterations with zero failures: flag for CI monitoring
- **Repro skipped** — Step 3 preconditions not met
- **Needs data inspection** — failure points to persisted state: `@skills/wt-cli/SKILL.md`
- **Needs owner** — assign to `<team>` because `<reason>`
- **Infra issue** — evidence: `<log lines>`
- **Already fixed** — specific fix commit identified AND fix verified at ≥ min iterations
- **Insufficient data** — `<what is missing and how to get it>`

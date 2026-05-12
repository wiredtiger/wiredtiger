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

## Step 1: Git History

→ **@paths/git-history.md** — read the file and spawn its `Agent()` block. Pass last-good/first-bad SHAs, failing file/function, and assertion text (from triage).

Returns: suspect commit list (SHA, date, ticket, reason flagged).

## Step 2: Codebase Lookup

Skip if triage produced no concrete signal (no assertion text, no function name, no `file:line`). Record "Codebase: skipped — no signal" and proceed to Output.

→ **@paths/codebase-lookup.md** — read the file and spawn its `Agent()` block. Pass assertion text, function name, and file:line (from triage).

Returns: assertion location (file:line), 5 lines above (verbatim), subsystem, prior tickets.

## Step 3: Reproduction

Skip if any of the following hold: triage has no concrete error signal / Step 2 subsystem is "unknown" / failure is already explained by a known fix commit. Record "Reproduction: skipped — `<reason>`".

→ **@paths/reproduction.md** — read the file and spawn its `Agent()` block. Pass test command, build variant, min iterations (from triage), and suspect commit (from Step 1).

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
| Jira description + comments (triage Step 1) | | |
| Evergreen log (triage Step 3) | | |
| Build Baron failure group (triage Step 2) | | |
| Sibling tickets (triage Step 2) | | |
| Source code for failing function / assertion (Step 2) | | |
| Git history in failure window (Step 1) | | |
| Local build + test run (Step 3) | | |

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

- **Reproduced — needs fix** — Step 3 reproduced; root cause identified: `@paths/build.md`
- **Reproduced — root cause unclear** — reproduced but mechanism not understood: continue source investigation
- **Not reproduced** — Step 3 met minimum iterations with zero failures: flag for CI monitoring
- **Repro skipped** — Step 3 preconditions not met: `@paths/build.md`
- **Needs data inspection** — failure points to persisted state: `@skills/wt-cli/SKILL.md`
- **Needs owner** — assign to `<team>` because `<reason>`
- **Infra issue** — evidence: `<log lines>`
- **Already fixed** — specific fix commit identified AND fix verified at ≥ min iterations
- **Insufficient data** — `<what is missing and how to get it>`

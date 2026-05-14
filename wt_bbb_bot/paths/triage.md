# Triage

Subagent path for initial ticket triage. Called from @paths/investigate.md.

**Purpose:** Gather everything useful for investigation.md in one pass. The output is
organized by what each investigation step needs — not by how the data was fetched.

**Quoting rule:** Error messages, stack frames, log lines, and engineer statements about
root cause must be quoted verbatim. Never paraphrase evidence. Summarize context around
quotes, but the quotes themselves must be exact.

## Inputs

- Ticket key (e.g. WT-XXXXX)

## Subagent

```
Agent(
  subagent_type="general-purpose",
  prompt="""
Triage a WiredTiger BF ticket. Your output is the starting package for a full
investigation — every piece of evidence you surface here saves the investigation
from having to re-fetch it.

**Quoting rule:** Error messages, stack frames, WT_VERB log lines, and engineer
statements must be quoted verbatim. Do not paraphrase evidence. Summarize context,
quote evidence.

Ticket: <WT-XXXXX>

## Fetch everything

### 1. Jira ticket + all comments
Call jira_get_issue + jira_get_issue_comments.

From the ticket: summary, status, priority, assignee, CAUSES/IS CAUSED BY links,
EVG task IDs from description, linked SERVER/WT tickets.

From every comment — read each one and extract:
- Githook comments: commit SHA, branch, full commit message. Flag if "revert" or "fix".
- Engineer comments: author, date, and quote every error message, log line, stack frame,
  or root cause statement verbatim. Also note: what fix was attempted, what happened,
  what was left unresolved.
- Build Baron comments: auto-resolution search terms, variants, task names.

Early exit: if a CAUSES link points to a closed ticket with a fix commit, or a comment
from the last 48h shows an active investigation — stop and return what you have, noting
the early exit reason.

### 2. Build Baron failure group
Call bb_get_bfg_by_task(task_id). Fallback: bb_search_bfgs, then bb_get_bfg.
Extract: failure group ID, last-good SHA, first-bad SHA, total failures, time window,
variants affected, CI blocker status, sibling BFs.

### 3. Evergreen logs
Follow @skills/evergreen/SKILL.md. Inputs: task IDs from Jira custom fields or EVG URL.
Extract verbatim: first error line, full stack trace with file:line for every frame,
wiredtiger_open config string, build variant, any WT_VERB log lines near the failure,
test CONFIG block if present.

### 4. Classify + count
Failure type (Crash/SIGABRT, Assertion, Hang/timeout, Data corruption, Flaky, Infra).
If infra: note and stop.
Min repro iterations: ≥5 failures → 10, 2–4 → 20, 1 → 30.

## Return

Organize the output as a handoff to investigation.md. Every section maps directly to
what a specific investigation step needs.

---

### Ticket context
- **Ticket:** WT-XXXXX — <summary>
- **Status / Priority / Assignee:** <values>
- **Age:** <days since first failure>
- **CAUSES links:** <list or none>
- **Failure type:** <Crash / Assertion / Hang / Data corruption / Flaky / Infra>

---

### Failure signature  ← used by: log evidence, evidence ledger, reproduction
- **Test:** <name and file>
- **First error (exact):** `"<quoted line>"`
- **Stack trace:** every frame with file:line:
  ```
  #N  <function> at <file>:<line>
  ...
  ```
- **wiredtiger_open config:** `<exact string, or "not found in log">`
- **Build variant:** <value or "unknown">
- **test/format CONFIG:** <exact key=value pairs if present in log, or "not extracted">

---

### Recurrence  ← used by: occurrence analysis, priority scoring
- **Total failures:** N over X days across Y variants
- **Failures last 7 days:** N
- **Failure pattern:** evenly spread / burst / single occurrence
- **Variants:** <list>
- **CI blocker:** yes / no / unknown
- **Siblings:** <list or none>
- **Last-good SHA:** <value or "unknown">
- **First-bad SHA:** <value or "unknown">
- **Min repro iterations:** 10 / 20 / 30

---

### All quoted evidence  ← used by: evidence ledger
Every error message, stack frame, log line, or engineer statement from any source
(Evergreen log, Jira description, Jira comments). One row per distinct piece of evidence.

| # | Quoted text (verbatim) | Source | 
|---|------------------------|--------|
| Q1 | `"<exact string>"` | <Evergreen log / Jira comment by X on DATE / Jira description> |
| Q2 | ... | ... |

---

### Prior fix/revert history  ← used by: git history, sibling review
Every commit SHA appearing in comments, in chronological order:

| SHA | Message | Type | Date |
|-----|---------|------|------|
| <sha> | <full commit message> | fix / revert / other | <date> |

If none: "none found in comments."

---

### Investigation leads  ← used by: all investigation steps

**For git history:**
- Window: <last-good SHA> to <first-bad SHA> (or: 90-day lookback from <date>)
- Files/functions to search: <from stack trace file:line or engineer comments>
- Assertion text to search: `"<exact string to grep in git log>"`
- Commits to examine first: <SHAs from comments, with reason>

**For codebase lookup:**
- Assertion text: `"<exact string>"`
- Function names in stack trace: <list with file:line>
- Files named in engineer comments: <list>
- Prior Jira search terms: `"<assertion text>"` in project WT

**For reproduction:**
- Command: <exact test command from log if available>
- CONFIG values: <exact key=value pairs>
- Build variant to match: <value>
- Known working stress parameters: <from engineer comments if any>

**For sibling/prior ticket review:**
- Related tickets: <WT-XXXXX / SERVER-XXXXX with one-line description>
- Fix commits to read: <SHAs from comments>
- Open questions from prior investigation: <list — direct quotes from engineer comments>
"""
)
```

## Returns to investigate.md

The full structured output above. Investigation.md should treat triage output as
pre-populated evidence — do not re-fetch what triage already returned.

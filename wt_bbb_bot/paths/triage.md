# Triage

Subagent path for initial ticket triage. Called from @paths/investigate.md.

## Inputs

- Ticket key (e.g. WT-XXXXX)

## Subagent

```
Agent(
  subagent_type="general-purpose",
  prompt="""
Triage a WiredTiger BF ticket. Return only the structured fields below — no raw log dumps.

Ticket: <WT-XXXXX>

## Step 1: Fetch WT ticket

Call jira_get_issue + jira_get_issue_comments for the ticket key. Extract:
- Summary / Status / Priority / Assignee
- EVG task IDs — all URLs or IDs in the description
- CAUSES / IS CAUSED BY links — any linked WT or SERVER tickets
- Last comment date
- Prior investigation in comments: yes / no / partial — one-line summary

Early exit: if a CAUSES link points to a closed ticket with a fix commit, or a comment from the
last 48h shows an active investigation — stop and return what you have, noting the early exit.

## Step 2: Check linked issues + Build Baron

- Read comments on any sibling tickets from the same task/commit.
- Call bb_get_bfg_by_task(task_id). Fallback: bb_search_bfgs, then bb_get_bfg.

Extract: failure group ID, last-good SHA, first-bad SHA, total failures, time window,
variants affected, CI blocker status.

## Step 3: Fetch Evergreen logs

Follow @skills/evergreen/SKILL.md for the full log-fetching escalation sequence.

Inputs: Failing Tasks, Failing Buildvariants, Evergreen Project, First Failing Revision
from Jira custom fields. Extract task ID from the Evergreen URL in the description if
those fields are absent.

Extract and carry forward: test name, first error line (exact quoted), stack trace top
frames, build variant, wiredtiger_open config.

## Step 4: Classify failure type

| Type | Characteristics |
|---|---|
| Crash / SIGABRT | Stack trace with signal or wiredtiger_abort |
| Assertion failure | WT_ASSERT, __wt_errx, or Python AssertionError |
| Hang / timeout | Task timeout, no progress in logs |
| Data corruption | verify failure, unexpected key/value |
| Flaky / intermittent | Passes sometimes, low failure rate |
| Environment / infra | OOM, disk full, network, agent crash |

Record the type. If infra: note in output and stop.

## Step 5: Recurrence + blast radius

From the Build Baron failure group (Step 2):
- How many distinct variants are affected?
- Failure rate over the last 7 days?
- Is this a CI blocker?

Set min repro iterations from BFG count (30 days):
  >= 5 failures → 10 iterations
  2–4 failures  → 20 iterations
  1 failure     → 30 iterations

## Return

- Ticket: WT-XXXXX — <summary>
- Status / Priority / Assignee: <values>
- CAUSES links: <list or none>
- Prior investigation: <yes — summary | no | partial — summary>
- Task ID(s): <from Jira or Evergreen URL>
- Last-good SHA: <from BFG or "unknown">
- First-bad SHA: <from BFG or "unknown">
- Total failures: N over X days across Y variants
- Failure pattern: <evenly spread / burst / single occurrence>
- Variants: <list>
- CI blocker: <yes / no / unknown>
- Siblings: <list or none>
- Test: <name and file>
- First error (exact): "<quoted line>"
- Stack trace: <top frames or "unavailable">
- wiredtiger_open config: <value or "not found in log">
- Build variant: <value or "unknown">
- Failure type: <from Step 4>
- Min repro iterations: <10 / 20 / 30>
"""
)
```

## Returns to investigate.md

- Ticket / Status / Priority / Assignee
- CAUSES links
- Prior investigation
- Task ID(s)
- Last-good SHA / First-bad SHA
- Total failures / Failure pattern / Variants / CI blocker / Siblings
- Test name / First error (exact) / Stack trace / wiredtiger_open config / Build variant
- Failure type
- Min repro iterations

# Triage Path

Initial triage of a single WT ticket. Runs before deep-dive investigation. Goal: determine if the work is already done, classify the failure, and establish urgency.

---

## Step 1: Fetch WT ticket

`jira_get_issue` for the WT key. Extract:
- Summary, Status, Priority, Assignee, Labels
- Linked Evergreen task or variant URLs in the description
- Linked issues (CAUSES / IS CAUSED BY / Relates to)
- Last comment date (is anyone already working this?)

## Step 2: Check linked issues and Build Baron

Before diving into logs, check if the work is already done:

- **Linked SERVER or WT tickets with CAUSES links** → root cause may be confirmed; go to Step 4 to verify.
- **Sibling tickets from the same task/commit** → read their comments; one may have a complete investigation.
- `bb_get_bfg_by_task` to get the failure group: how many variants are failing, how often, and over what time window.

If a confirmed root cause exists, skip to Step 4 to verify and summarize.

## Step 3: Fetch Evergreen task logs

→ **@paths/investigate.md Step 3** for the full log-fetching escalation sequence and extraction rules.

Inputs come from the WT ticket custom fields: `Failing Tasks`, `Failing Buildvariants`, `Evergreen Project`, `First Failing Revision`. If the task ID isn't present as a field, extract it from the Evergreen URL in the ticket description.

Carry forward: first error line, stack-trace summary, failure type, affected subsystem.

## Step 4: Classify failure type

| Type | Characteristics | Next step |
|---|---|---|
| Crash / SIGABRT | Stack trace with signal or `wiredtiger_abort` | @paths/investigate.md |
| Assertion failure | `WT_ASSERT`, `__wt_errx`, or Python `AssertionError` | @paths/investigate.md |
| Hang / timeout | Task timeout, no progress in logs | @paths/investigate.md |
| Data corruption | `verify` failure, unexpected key/value | @paths/investigate.md |
| Flaky / intermittent | Passes sometimes, low failure rate | @paths/build.md to measure rate |
| Environment / infra | OOM, disk full, network, agent crash | Note and close as infra issue |

## Step 5: Check recurrence and blast radius

From the Build Baron failure group (Step 2):
- How many distinct variants are affected?
- Failure rate over the last 7 days?
- Is this a CI blocker (gating trunk or a release)?

This determines urgency — carry forward into the output.

---

## Context handoff → @paths/investigate.md

Populate every field before handing off. Write "unknown" only if the source was attempted and unavailable.

| Field | Value |
|---|---|
| Ticket | `WT-XXXXX` — `<summary>` |
| Task ID(s) | `<from Jira custom fields or Evergreen URL>` |
| Last-good SHA | `<from BFG, or "unavailable">` |
| First-bad SHA | `<from BFG, or "unavailable">` |
| First error line | `<exact quoted string, or "unavailable">` |
| Stack trace (top frames) | `<or "unavailable">` |
| Failure type | `<crash / assertion / hang / corruption / flaky / infra>` |
| Subsystem | `<name, or "unknown">` |
| Min repro iterations | `<10 / 20 / 30 — from BFG count>` |
| CI blocker | `<yes / no / unknown>` |
| Siblings found | `<list of WT-XXXXX keys, or "none">` |
| Prior investigation | `<yes — one-line summary / no / partial — one-line summary>` |

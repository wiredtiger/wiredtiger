---
name: wt-analyze
description: Triage WiredTiger build failure (BF) tickets — fetch Jira and Evergreen context, classify the failure type, identify root cause, and drive resolution.
when_to_use: WiredTiger BF triage, Evergreen test failure investigation, WT crash or assertion analysis, BF priority ranking, local reproduction
argument-hint: "BF-XXXXX"
owner: jie.chen@mongodb.com
---

# WiredTiger BF Analyzer

Triage WiredTiger build failure tickets end-to-end: fetch context from Jira, Evergreen, and
Build Baron, classify the failure, form a root cause hypothesis, and produce a Jira-ready
investigation summary.

# Routing

| Need | Go to |
|---|---|
| Initial triage of one BF ticket | This file — Steps 1–7 below |
| Unclear failure / "why did this happen?" | @paths/investigate.md — Phase 0 |
| Deep-dive root cause investigation | @paths/investigate.md — Phase 1+ |
| Rank and prioritize multiple open BFs | @paths/priority.md |
| Reproduce locally / run test/format | @paths/build.md |
| Inspect a WT data directory or WAL | @skills/wt-cli/SKILL.md |
| Load, search, or comment on a Jira ticket | @skills/jira/SKILL.md |
| Structured output format / Jira comment | @reference/output-template.md |
| Escalation order and good defaults | @reference/workflow.md |
| Inspect WT pages in SLS / disagg storage | @skills/disagg-page-inspection/SKILL.md |
| Triage a HELP ticket with FTDC data | @skills/help-ticket-triage/SKILL.md |
| test/format runs, tracing, parallel repro | @skills/wiredtiger-test-format/SKILL.md |
| Investigate a commit, blame a function, find a PR | @skills/github/SKILL.md |
| Root cause methodology (before fixing anything) | @skills/systematic-debugging/SKILL.md |

# MCP Tools

| Tool | Used For |
|---|---|
| `mcp__devprod-mcp-gateway__jira_get_issue` | BF ticket details, status, assignee, links |
| `mcp__devprod-mcp-gateway__jira_get_issue_comments` | Prior investigation history |
| `mcp__devprod-mcp-gateway__jira_search_issues` | Sibling BFs, related SERVER tickets |
| `mcp__devprod-mcp-gateway__jira_add_comment` | Post investigation summary (only with user confirmation) |
| `mcp__devprod-mcp-gateway__bb_get_bf` | Build Baron failure details |
| `mcp__devprod-mcp-gateway__bb_get_bfg` | Build failure group — recurrence, variants |
| `mcp__devprod-mcp-gateway__bb_get_bfg_by_task` | Failure group for a given Evergreen task |
| `mcp__devprod-mcp-gateway__bb_search_bfgs` | Search for related failure groups |
| `mcp__devprod-mcp-gateway__evg_get_task_log_summary` | Quick error scan of task logs |
| `mcp__devprod-mcp-gateway__evg_get_raw_task_logs` | Full logs when summary is not enough |
| `mcp__devprod-mcp-gateway__evg_get_test_results_summary` | Which tests passed/failed |
| `mcp__devprod-mcp-gateway__evg_get_test_results_detailed` | Raw test output and error patterns |
| `mcp__devprod-mcp-gateway__evg_get_patch_failed_jobs` | All failed tasks in an Evergreen patch |
| `mcp__claude_ai_Glean_via_MCP__search` | Internal design docs, runbooks, Slack threads |

# Arguments

Parse `$ARGUMENTS`:
- A BF ticket key (e.g., `BF-12345`)
- If no argument, ask the user for the BF ticket key

# Process

## Step 1: Fetch BF ticket

`jira_get_issue` for the BF key. Extract:
- Summary, Status, Priority, Assignee, Labels
- Linked Evergreen task or variant URLs in the description
- Linked issues (CAUSES / IS CAUSED BY / Relates to)
- Last comment date (is anyone already working this?)

## Step 2: Check linked issues and Build Baron

Before diving into logs, check if the work is already done:

- **Linked SERVER or WT tickets with CAUSES links** → root cause may be confirmed; go to
  Step 4 to verify.
- **Sibling BFs from the same task/commit** → read their comments; one may have a complete
  investigation while the other is untouched.
- `bb_get_bfg_by_task` to get the failure group: how many variants are failing, how often,
  and over what time window.

If a confirmed root cause exists, skip to Step 6 to verify and summarize.

## Step 3: Fetch Evergreen task logs

`evg_get_task_log_summary` for the failing task ID from the BF description.

If the summary does not show the first error clearly, use `evg_get_raw_task_logs` with
`log_type=task`.

Extract:
- First error line and surrounding context (20 lines before/after)
- Stack trace (if crash or assertion)
- Test name and file
- Whether the failure is deterministic or appears intermittent in the log

## Step 4: Classify failure type

| Type | Characteristics | Analysis path |
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
- Is this gating a release or blocking trunk?

This determines urgency — include in the output.

## Step 6: Form root cause hypothesis

Based on the first error, stack trace, and any confirmed linked cause:
- **What failed**: test name, assertion text, or crash signal
- **Where it failed**: source file and line if visible in the stack
- **Why it likely failed**: the narrowest plausible explanation
- **Confidence**: Low / Medium / High

## Step 7: Produce output

Use the template at @reference/output-template.md.

Always end with:
- **Recommended next action** (one of: investigate deeper → @paths/investigate.md,
  reproduce locally → @paths/build.md, assign to owner, close as infra, already fixed)
- **Jira comment ready** (offer to post with `jira_add_comment` — always confirm first)

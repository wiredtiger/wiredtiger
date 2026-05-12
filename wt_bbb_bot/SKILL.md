---
name: bug-bash-bot
description: Triage WiredTiger build failure (BF) tickets — fetch Jira and Evergreen context, classify the failure type, identify root cause, and drive resolution.
when_to_use: WiredTiger BF triage, Evergreen test failure investigation, WT crash or assertion analysis, BF priority ranking, local reproduction
argument-hint: "BF-XXXXX"
owner: jie.chen@mongodb.com
allowed-tools:
  - mcp__devprod-mcp-gateway__jira_get_issue
  - mcp__devprod-mcp-gateway__jira_get_issue_comments
  - mcp__devprod-mcp-gateway__jira_search_issues
  - mcp__devprod-mcp-gateway__jira_add_comment
  - mcp__devprod-mcp-gateway__bb_get_bf
  - mcp__devprod-mcp-gateway__bb_get_bfg
  - mcp__devprod-mcp-gateway__bb_get_bfg_by_task
  - mcp__devprod-mcp-gateway__bb_search_bfgs
  - mcp__devprod-mcp-gateway__evg_get_task_log_summary
  - mcp__devprod-mcp-gateway__evg_get_raw_task_logs
  - mcp__devprod-mcp-gateway__evg_get_test_results_summary
  - mcp__devprod-mcp-gateway__evg_get_test_results_detailed
  - mcp__devprod-mcp-gateway__evg_get_patch_failed_jobs
  - mcp__devprod-mcp-gateway__evg_list_user_recent_patches
  - mcp__devprod-mcp-gateway__evg_get_inferred_project_ids
  - mcp__devprod-mcp-gateway__evg_download_task_artifacts
  - mcp__devprod-mcp-gateway__git_log
  - mcp__devprod-mcp-gateway__git_blame
  - mcp__devprod-mcp-gateway__git_search
  - mcp__devprod-mcp-gateway__git_diff
  - mcp__devprod-mcp-gateway__git_show
  - mcp__devprod-mcp-gateway__confluence_get_page
  - mcp__devprod-mcp-gateway__confluence_get_page_by_title
  - mcp__claude_ai_Glean_via_MCP__search
  - Bash
  - Read
  - Write
  - Edit
  - Agent
---

# Rules

- Follow Steps 1–8 below exactly, in order. Do not skip steps or deviate from the process.
- If a step cannot be completed (e.g., no Evergreen task URL exists), note the skip explicitly before continuing.
- Do not implement code fixes or take actions beyond Step 8 without explicit user instruction.
- Always end at Step 8: attempt reproduction, then offer to post the full output (RCA + reproducer) as a Jira comment.

## Step completion checklist

Before moving to the next step, confirm the current step is done by printing a one-line status:

```
✓ Step N complete — <one sentence summary of what was found/done>
```

Do not proceed to Step N+1 without printing this line. This makes the step sequence visible and auditable.

**Critical ordering rules:**
- Step 7 (reproduce) MUST be completed before Step 8 (produce output). Never jump from Step 6 to Step 8.
- After Step 8, the Jira comment offered to the user MUST be the combined output: Step 8 template (using @templates/bf-comment.md) + Step 7 reproducer results. Do not offer a freeform comment.

## Explore agent

**Use an Explore subagent for ALL source code investigation. This is a hard rule — no exceptions.**

Never use the Read tool, Bash grep, or any inline tool to look at source files for investigative purposes. It does not matter how targeted or "quick" the lookup seems. Every investigative source read must go through an Explore agent. Rationalizations like "I know exactly where to look" or "it's just one file" are not exceptions — spawn the agent.

The only inline reads permitted are for files you are about to edit (reading the file immediately before an Edit call). Everything else — understanding code, tracing a call path, checking if a guard exists, reading an assertion — goes through Explore.

When to spawn an Explore agent:
- Locating where an assertion, macro, or function is defined
- Tracing a stack frame through source to understand what a line does
- Finding recent changes to a file or subsystem (complement to git_blame)
- Checking whether a fix or guard already exists in the current source
- Understanding a subsystem's invariants before forming a hypothesis
- Any question of the form "what does this code do?" or "where is X?"

How to invoke:
```
Agent(subagent_type="Explore", prompt="<specific question about the WiredTiger source>. Search breadth: <quick|medium|very thorough>.")
```

Return the Explore agent's findings as evidence in the root cause hypothesis (Step 6) and the source-check result (Step 8a).

## Priority agent

**Always run priority scoring in a subagent.** Never score a ticket inline. Spawn a `claude` subagent, pass it the full investigation output, and let it apply `@paths/priority.md` in isolation. This keeps the scoring step's context window separate from the investigation context.

When to spawn a priority agent:
- After Step 6 (root cause hypothesis) is complete for one or more tickets
- When the user asks to rank or prioritize a set of open BFs
- When `@paths/priority.md` is the next action in the routing table

How to invoke:
```
Agent(prompt="""You are a WiredTiger BF priority scorer. Apply the scoring rules in @paths/priority.md exactly.

## Investigation output

<paste the full investigation output here>

## Task

Score this ticket 0–100, assign a severity label (Critical / High / Medium / Low / Minimal), write a one-paragraph rationale, and restate the next action with urgency context. Follow the output format in @paths/priority.md Step 3 exactly.""")
```

Return the priority agent's score, label, rationale, and next action as the **Priority Assessment** section of the Step 8 output template.

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
| Rank and prioritize multiple open BFs | Priority agent (subagent) — see ## Priority agent above |
| Reproduce locally / run test/format | @paths/build.md |
| Inspect a WT data directory or WAL | @skills/wt-cli/SKILL.md |
| Load, search, or comment on a Jira ticket | @skills/jira/SKILL.md |
| Fetch CI logs, test results, or patch failures | @skills/evergreen/SKILL.md |
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

Use the evergreen skill (@skills/evergreen/SKILL.md) to fetch task logs, test results, and
patch failure context. The skill handles WT-specific failure classification, the log-prefix
→ subsystem map, and the Antithesis special-case.

Inputs come from the BF custom fields you read in Step 1: `Failing Tasks`,
`Failing Buildvariants`, `Evergreen Project`, `First Failing Revision`. If the task ID
isn't present as a field, extract it from the Evergreen URL in the BF description (the long
hex string in the path).

Hand back to this flow: the first error line, stack-trace summary, failure type, affected
subsystem, and recommendation for which path/skill to invoke next.

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
- Is this a CI blocker (gating trunk or a release)?

This determines urgency — include in the output.

## Step 6: Form root cause hypothesis

Follow @skills/systematic-debugging/SKILL.md Phases 1–3 using the evidence from Steps 1–5.
Do not propose a fix or skip to Step 7 until Phase 3 produces a confirmed hypothesis.

Output:
- **What failed**: test name, assertion text, or crash signal
- **Where it failed**: source file and line if visible in the stack
- **Why it likely failed**: the narrowest plausible explanation
- **Confidence**: Low / Medium / High

## Step 7: Reproduce the bug locally

Follow @paths/build.md.

**7a — Check if the bug is already fixed in current source:**

Spawn an Explore agent to check whether the specific guard, assertion, or function the fix should touch already exists. If the fix is already present, skip to reporting "already fixed" and skip the build.

**7b — Build:**

Use an incremental build if `build/` exists. Match the CI variant (ASan if CI used ASan, etc.):
→ @paths/build.md for commands.

**7c — Choose the repro method:**

| Situation | Method |
|---|---|
| A concrete deterministic Python scenario is known (from the ticket or WT-17278-style test) | Write and run a Python test: `python3 ../test/suite/run.py <test> -j1` |
| No deterministic scenario but a test/format config is known | Run `./t -c CONFIG <overrides> runs.timer=5` — 3–5 iterations |
| Neither is known | State "no repro attempted — insufficient scenario" and move on |

Prefer the Python path: it runs in seconds, is deterministic, and becomes a regression test.

**7d — Report outcome:**

- **Reproduced:** exact command, failure output (first error line + stack), iteration it failed on
- **Not reproduced:** command run, N iterations, "bug may be already fixed or scenario requires specific timing"
- **Already fixed:** evidence in source (file:line where the guard now exists)

**7e — Write the reproducer:**

If reproduced, write the minimal Python test or test/format one-liner that triggers the failure.
This becomes the `Reproducer` section of the Jira comment.

## Step 8: Produce output

Use the template at @reference/output-template.md. Include the reproducer result from Step 7.

**After Step 8:** offer to post the full output (RCA + reproducer) with `jira_add_comment` — always confirm first.

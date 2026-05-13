---
name: bug-bash-bot
description: Triage WiredTiger build failure (WT) tickets — fetch Jira and Evergreen context, classify the failure type, identify root cause, and drive resolution.
argument-hint: "WT-XXXXX"
owner: jie.chen@mongodb.com
allowed-tools:
  - mcp__devprod-mcp-gateway__*
  - mcp__claude_ai_Glean_via_MCP__search
  - Bash
  - Read
  - Write
  - Edit
  - Agent
---

# Rules

- Run Steps 1–4 in order. Note any skipped step explicitly before continuing.
- After Step 3, offer to post the full output as a Jira comment — always confirm first.
- Print a one-line status before each step transition: `✓ Step N complete — <summary>`

# Routing

| Need | Go to |
|---|---|
| Triage + root cause + reproduce | @paths/investigate.md |
| Priority scoring | @paths/priority.md |
| Results / Jira comment | @reference/output-template.md |
| Codebase knowledge | @reference/codebase.md |

# Process

Parse `$ARGUMENTS` for a WT ticket key (e.g., `WT-12345`). If absent, ask the user.

## Step 1: Investigate

```
Agent(
  subagent_type="general-purpose",
  prompt="""
Investigate a WiredTiger BF ticket end-to-end. Follow @paths/investigate.md exactly — all steps in order, no skipping without noting the reason.

Ticket: <WT-XXXXX>

Run the full flow: triage (Steps 1–5) → git history (Step 6) → codebase lookup (Step 7) → reproduction (Step 8).

Return only the populated Output section from @paths/investigate.md. Do not post to Jira or modify any external state.

At the very end of your response, append this line (fill in your best estimate):
`[usage: ~<N> input tokens / ~<M> output tokens]`
"""
)
```

Returns: populated Output section (Jira context, occurrence analysis, log evidence, git history, codebase, reproduction, working theory, recommended fix, next action) + usage line.

## Step 2: Priority

```
Agent(
  subagent_type="general-purpose",
  prompt="""Apply @paths/priority.md to score this WT ticket.

## Investigation output
<paste Step 1 output here>

At the very end of your response, append this line (fill in your best estimate):
`[usage: ~<N> input tokens / ~<M> output tokens]`
"""
)
```

Returns: score (0–100), label (P1–P5), one-paragraph rationale, next action with urgency context + usage line.

## Step 3: Post results

Using `@reference/output-template.md`, compose the final comment from Step 1 (investigation output + reproducer) and Step 2 (Priority Assessment). Show the exact text to the user and confirm before posting with `jira_add_comment`.

## Step 4: Learn

```
Agent(
  subagent_type="general-purpose",
  prompt="""
Update @reference/codebase.md with high-level architectural knowledge learned from this investigation.

## Investigation output
<paste Step 1 output here>

Rules:
- Only write subsystem ownership, component relationships, data flow, and system-level invariants.
- Never write function names, struct names, field names, lock names, line numbers, or config strings — those belong in source, not here.
- Entries must remain true even if the implementation is refactored.

Follow the entry format in codebase.md. Add new entries, correct stale ones, remove entries that are no longer true. Return a one-line summary of what was saved.

At the very end of your response, append this line (fill in your best estimate):
`[usage: ~<N> input tokens / ~<M> output tokens]`
"""
)
```

## Step 5: Write reasoning log

Write a reasoning log to `wt_bbb_bot/logs/<ticket>.md` using the Write tool. Create the `logs/` directory first if it does not exist (use Bash: `mkdir -p wt_bbb_bot/logs`).

The file captures *why* each decision was made — not a summary of findings, but the reasoning trail used to reach them. This is used to improve and fine-tune these prompts.

```markdown
# Reasoning Log: <WT-XXXXX>

**Date**: <ISO 8601 timestamp>
**Model**: claude-opus-4-7

---

## Token usage

| Step | Input tokens | Output tokens |
|------|-------------|---------------|
| Step 1 — Investigate | ~<N> | ~<M> |
| Step 2 — Priority    | ~<N> | ~<M> |
| Step 4 — Learn       | ~<N> | ~<M> |
| **Total**            | **~<N>** | **~<M>** |

> Note: these are model self-estimates — actual counts may vary ±10–20%.

---

## Triage decisions

- **Failure type**: <type> — why: <what specific evidence in the log drove this classification>
- **Early exit**: <yes — reason | no>
- **Key evidence used**: <exact quoted lines or fields that mattered most>
- **Evidence that was absent or ambiguous**: <what was missing or unclear>

## Investigation decisions

- **Git history** (Step 6): <what was searched, what was found, or why skipped>
- **Codebase lookup** (Step 7): <which subsystems were examined and why those specifically>
- **Reproduction** (Step 8): <approach chosen and the reasoning — why those iterations, that config>
- **Working theory**: <the theory and what evidence supports vs. contradicts it>

## Priority decisions

- **Score**: <0–100> — **Label**: <P1–P5>
- **Factors that raised the score**: <list>
- **Factors that lowered the score**: <list>

## Prompt path coverage

- **Steps taken in order**: <list>
- **Steps skipped**: <step — reason for each>
- **Judgment calls made outside the prompt**: <any decisions not explicitly guided by the prompts>

## Prompt gaps noticed

<List any cases where the prompt was ambiguous, gave conflicting guidance, or lacked instruction for the situation encountered. Be specific — quote the ambiguous prompt text if possible. Write "none" if the prompts covered everything.>
```

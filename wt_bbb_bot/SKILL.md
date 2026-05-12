---
name: bug-bash-bot
description: Triage WiredTiger build failure (WT) tickets — fetch Jira and Evergreen context, classify the failure type, identify root cause, and drive resolution.
when_to_use: WiredTiger BF triage, Evergreen test failure investigation, WT crash or assertion analysis, WT priority ranking, local reproduction
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

- Run Steps 1–5 in order. Note any skipped step explicitly before continuing.
- Do not implement code fixes without explicit user instruction.
- After Step 4, offer to post the full output as a Jira comment — always confirm first.
- Print a one-line status before each step transition: `✓ Step N complete — <summary>`

## Explore agent

Before spawning an Explore agent, check `@reference/codebase.md` for orientation — use it to form a targeted question, then verify with source.

```
Agent(subagent_type="Explore", prompt="<question about WiredTiger source>. Search breadth: <quick|medium|very thorough>.")
```

# Routing

| Need | Go to |
|---|---|
| Initial triage (fetch, classify, recurrence) | @paths/triage.md |
| Root cause + reproduce | @paths/investigate.md |
| Priority scoring | @paths/priority.md |
| Results / Jira comment | @reference/output-template.md |
| Codebase knowledge | @reference/codebase.md |

# Process

Parse `$ARGUMENTS` for a WT ticket key (e.g., `WT-12345`). If absent, ask the user.

## Step 1: Triage

→ **@paths/triage.md** — fetch ticket, check linked issues and Build Baron, fetch Evergreen logs, classify failure type, establish recurrence and blast radius.

## Step 2: Investigate

→ **@paths/investigate.md** — root cause hypothesis, reproduction, and fix verification.

## Step 3: Priority

Spawn a `claude` subagent with the full investigation output and instruct it to apply `@paths/priority.md` in isolation. Return the score, label, rationale, and next action as the **Priority Assessment** in Step 4.

```
Agent(prompt="""Apply @paths/priority.md to score this WT ticket.

## Investigation output
<paste the full investigation output here>
""")
```

## Step 4: Post results

Use `@reference/output-template.md`. Include the reproducer from Step 2 and Priority Assessment from Step 3. Offer to post with `jira_add_comment` — confirm with the user first.

## Step 5: Learn

→ **@reference/codebase.md** — follow the entry format there. Print a one-line summary of what was saved.

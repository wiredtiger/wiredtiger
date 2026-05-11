---
name: jira
description: Read Jira tickets and post investigation comments. Use when loading BF tickets, reading comments and status, searching for related issues, or adding analysis comments. Read-only + comment only — does not create, update, or transition issues.
when_to_use: loading BF tickets, reading comments, searching for related tickets, posting investigation summaries
---

# Jira Skill

## Preferred Method: devprod MCP Gateway

Use `mcp__devprod-mcp-gateway__jira_*` tools when available. No CLI installation or token required.

### Fetch ticket details

```
mcp__devprod-mcp-gateway__jira_get_issue(issue_key="BF-12345")
```

Extract: Summary, Status, Priority, Assignee, Labels, linked tickets (CAUSES / IS CAUSED BY / Relates to), Evergreen task URLs in description.

### Read comments

```
mcp__devprod-mcp-gateway__jira_get_issue_comments(issue_key="BF-12345")
```

Check: Is anyone already investigating? Has a root cause been proposed? Are there reproduction steps?

### Search for related tickets

```
mcp__devprod-mcp-gateway__jira_search_issues(jql="<query>")
```

Common JQL patterns for WT BFs:
```
# Open WT BFs unowned
project = BF AND component = "WiredTiger" AND status not in (Closed, "Won't Fix") AND assignee is EMPTY

# BFs touching a specific test or function
project in (BF, WT) AND text ~ "test_checkpoint" ORDER BY created DESC

# Sibling BFs from the same causal commit
project = BF AND text ~ "<commit SHA>"

# Recent BFs for a specific variant
project = BF AND component = "WiredTiger" AND text ~ "<variant name>" AND created >= -7d
```

### Post a comment

Only after the working theory is clear. Always show the full comment to the user and get explicit confirmation before calling.

```
mcp__devprod-mcp-gateway__jira_add_comment(issue_key="BF-12345", comment="<text>")
```

Use the template at @../../templates/bf-comment.md for investigation summaries.

---

## Fallback Method: Jira CLI

Use only if the MCP gateway is unavailable. Requires `brew install ankitpokhrel/jira-cli/jira-cli` and `jira init` with a Personal Access Token.

### Fetch ticket details

```bash
# Full view
jira issue view BF-12345

# Plain text (better for parsing)
jira issue view BF-12345 --plain

# Include comments
jira issue view BF-12345 --comments 5

# Raw JSON
jira issue view BF-12345 --raw
```

### Search issues

```bash
# List open WT BFs
jira issue list -p BF -q "component = 'WiredTiger' AND status not in (Closed, \"Won't Fix\")"

# Custom JQL
jira issue list -q "project = BF AND text ~ 'test_checkpoint' ORDER BY created DESC"
```

### Post a comment

```bash
jira issue comment add BF-12345 "Working theory: checkpoint assertion triggered by concurrent eviction. Repro confirmed in 3/10 format runs."
```

---

## Standard comment structure

### Analysis
What failed and where.

### Evidence
Commands, logs, code pointers, or reproduction notes.

### Working theory
Most likely cause and confidence level.

### Next steps
Exact follow-up actions.

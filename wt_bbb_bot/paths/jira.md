# Jira Path

Use this path for:
- loading BF tickets
- reading comments / status
- adding analysis comments
- linking code investigation back to a ticket

## Tools

| Tool | Used For |
|---|---|
| `mcp__devprod-mcp-gateway__jira_get_issue` | Ticket summary, description, labels, status, assignee, linked issues |
| `mcp__devprod-mcp-gateway__jira_get_issue_comments` | Investigation history and prior comments |
| `mcp__devprod-mcp-gateway__jira_search_issues` | Find sibling BFs or related SERVER/WT tickets via JQL |
| `mcp__devprod-mcp-gateway__jira_add_comment` | Post analysis summary — always confirm with user first |
| `mcp__devprod-mcp-gateway__jira_transition_issue` | Change ticket status — always confirm with user first |

## Workflow

### Step 1: Load the ticket

`jira_get_issue` — extract:
- Summary, Status, Priority, Assignee, Labels
- Failing test or symptom from the description
- Suspected area or component
- Reproduction clues (Evergreen task URLs, configs)
- Linked tickets (CAUSES / IS CAUSED BY / Relates to)

### Step 2: Read the comments

`jira_get_issue_comments` — check:
- Is anyone already investigating?
- Has a root cause been proposed?
- Are there reproduction steps recorded?

### Step 3: Search for related tickets

Common JQL patterns for WT BFs:
```
# Open WT BFs unowned
project = BF AND component = "WiredTiger" AND status not in (Closed, "Won't Fix") AND assignee is EMPTY

# BFs touching a specific test or function
project in (BF, WT) AND text ~ "test_checkpoint" ORDER BY created DESC

# Sibling BFs from the same causal commit
project = BF AND text ~ "<commit SHA>"
```

### Step 4: Post a comment

Only after the working theory is clear. Keep comments short and evidence-based.

Use the template at @../templates/bf-comment.md.

Always show the full comment to the user and get explicit confirmation before calling
`jira_add_comment`.

## Standard comment structure

### Analysis
What failed and where.

### Evidence
Commands, logs, code pointers, or reproduction notes.

### Working theory
Most likely cause and confidence level.

### Next steps
Exact follow-up actions.

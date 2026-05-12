# Git History

Subagent path for git history analysis. Called from @paths/investigate.md Step 6.

## Inputs (from investigate.md Steps 2–3)

- Last-good SHA
- First-bad SHA
- Failing file: `src/<path>.c`
- Failing function: `<function>`
- Assertion text: `<text>`

## Subagent

```
Agent(
  subagent_type="general-purpose",
  prompt="""
Run git history analysis for a WiredTiger BF. Do not return raw log output — summarize only.

Inputs:
- Last-good SHA: <sha>
- First-bad SHA: <sha>
- Failing file: src/<path>.c
- Failing function: <function>
- Assertion text: <text>

Tasks:
1. Commits touching the failing file between the two SHAs — list SHA, author, date,
   one-line summary, linked ticket (WT-XXXXX from message).
2. Blame the failing file at the assertion line — which commit last changed it?
3. Search commits by assertion text or ticket key if the window is large.
4. If the window is small (< 10 commits), diff between the two SHAs filtered to the failing file.

Return only: a ranked list of suspect commits (SHA, date, ticket, reason flagged),
or "no suspect commit identified" if none found.
"""
)
```

## Returns to investigate.md

- **Suspect commit:** `<SHA — author, date, summary, or "none identified">`
- **Linked ticket:** `<WT-XXXXX or SERVER-XXXXX from commit message, or none>`
- **In failure window:** `<yes / no / unverified>`

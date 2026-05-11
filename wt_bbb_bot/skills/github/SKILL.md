---
name: github
description: Git and GitHub investigation for WiredTiger — find commits in a failure window, blame a function, search for PRs or commits by SHA or ticket key, correlate a regression to a specific change.
when_to_use: investigating which commit caused a regression, blaming a function or file, finding a PR from a ticket key, verifying a commit is in a failure window
---

# Git Investigation

Use this skill when you need to trace a WiredTiger failure back to a specific commit,
understand when a function changed, or correlate a BF to a code change.

## Tools

| Tool | Used For |
|---|---|
| `mcp__devprod-mcp-gateway__git_log` | Walk commit history between two SHAs or refs |
| `mcp__devprod-mcp-gateway__git_blame` | Find when a specific line or function last changed |
| `mcp__devprod-mcp-gateway__git_search` | Search commits by message, ticket key, or author |
| `mcp__devprod-mcp-gateway__git_diff` | Diff between two commits or refs |
| `mcp__devprod-mcp-gateway__git_show` | Inspect a single commit — message, diff, files changed |

---

## Workflow

### Use case 1: Is a suspect commit in the failure window?

When a BF has a causal commit SHA or a linked SERVER/WT ticket, verify the commit
actually falls between the last good and first bad builds.

1. Get the last-good and first-bad version SHAs from the Build Baron failure group or
   Evergreen task history.
2. Search for the suspect commit:
   ```
   git_search: query="<SHA or SERVER-XXXXX or WT-XXXXX>"
   ```
3. Confirm the commit date falls between last-good and first-bad.
4. If it does not → hypothesis is falsified, proceed with fresh investigation.

---

### Use case 2: Blame a function or file

When a stack trace points to a specific file and line, find when it last changed and why.

```
git_blame: file="src/third_party/wiredtiger/src/txn/txn.c" line=<N>
```

Then inspect the commit:
```
git_show: sha="<commit from blame>"
```

Look for:
- The WT or SERVER ticket in the commit message
- Whether the change is in the exact line that asserted or crashed
- Any associated test changes that might hint at the intended invariant

---

### Use case 3: Find all commits touching a subsystem between two points

When you want to narrow the regression window to a specific subsystem:

```
git_log: from="<last_good_sha>" to="<first_bad_sha>" path="src/third_party/wiredtiger/src/txn/"
```

Scan the results for commits that:
- Touch the failing function or file
- Reference a relevant WT/SERVER ticket
- Have a message matching the failure signature (e.g. "checkpoint", "eviction", "timestamp")

---

### Use case 4: Correlate a commit SHA to a ticket

When Evergreen or Build Baron gives you a raw SHA and you need the ticket context:

```
git_show: sha="<SHA>"
```

The commit message should reference a WT-XXXXX or SERVER-XXXXX ticket. Use that ticket
key to search Jira for the full context and any linked BFs.

---

### Use case 5: Diff between good and bad versions

When you want to see everything that changed between the last-good and first-bad build:

```
git_diff: from="<last_good_sha>" to="<first_bad_sha>"
```

Filter mentally (or by path) to the subsystem identified in the investigation.

---

## Output format

### Commit finding
- **SHA:** `<commit SHA>`
- **Author / date:** ...
- **Ticket:** WT-XXXXX or SERVER-XXXXX
- **In failure window:** yes / no
- **Relevant change:** one sentence describing what the commit did

### Conclusion
One paragraph — does the commit explain the failure? Why or why not?

### Next checks
1. ...
2. ...

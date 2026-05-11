# Priority Path

Use this path when the user wants to rank multiple open WiredTiger BFs.

## Input

One or more of:
- Explicit BF keys (`BF-12345, BF-12346, ...`)
- A Jira JQL query (`project = BF AND component = "WiredTiger" AND status != Done`)
- A Build Baron search query (test name, variant, or failure signature)

## Tools

- `jira_search_issues` — list open BFs, filter by component/team/label
- `jira_get_issue` — details for each BF in the set
- `bb_get_bfg_by_task` or `bb_search_bfgs` — recurrence and blast radius per BF
- `evg_get_task_log_summary` — quick failure signature for unknown BFs

## Priority criteria (in order)

1. **Blast radius** — How many distinct variants/builds does it block?
2. **Recurrence** — Failures in the last 7 days; is it trending up?
3. **Age** — Days open without a fix or investigation comment.
4. **Release impact** — Is this gating a release branch or blocking trunk?
5. **Test importance** — Critical path tests: checkpoint, txn, eviction, rollback-to-stable, disagg.
6. **Owner** — Unowned BFs with high blast radius are highest urgency.

## Output format

### Priority list

| Rank | BF | Summary | Blast radius | Recurrence (7d) | Age | Owner |
|------|----|---------|--------------|-----------------|-----|-------|

### Top priority rationale
One paragraph on the #1 item.

### Next action per top-3
1. BF-XXXXX: `<one concrete action>`
2. BF-XXXXX: `<one concrete action>`
3. BF-XXXXX: `<one concrete action>`

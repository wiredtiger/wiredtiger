# Output Template

Every investigation should end with this structure. Use it for both console output and
Jira comments (see @../templates/bf-comment.md for the Jira-formatted version).

---

## BF Summary

**Ticket:** BF-XXXXX — `<summary>`
**Status:** `<current Jira status>`
**Variants affected:** `<N variants>` — `<list or "all">`
**Recurrence (7d):** `<N failures>`
**Age:** `<N days open>`

---

## Failure Classification

| Field | Value |
|---|---|
| Type | Crash / Assertion / Hang / Corruption / Flaky / Infra |
| Subsystem | e.g. checkpoint, eviction, txn, rollback-to-stable, disagg |
| First error | `<exact error line>` |
| Stack (if any) | `<top 3–5 frames>` |

---

## Root Cause Hypothesis

`<One paragraph. State what failed, where, and the most likely why.>`

**Confidence:** Low / Medium / High
**Key uncertainty:** `<what would change the conclusion>`

---

## Evidence

- **EVG task:** `<task ID or URL>`
- **Log line:** `<file:line or log excerpt>`
- **Related tickets:** `<BF-XXXXX, SERVER-XXXXX, WT-XXXXX>`
- **Prior occurrences:** `<yes/no — link if yes>`

---

## Recommended Next Action

`<One of the following>`
- Investigate deeper → @paths/investigate.md (`<specific question to answer>`)
- Reproduce locally → @paths/build.md (`<exact repro command or config>`)
- Assign to owner: `<team or person>` (`<reason>`)
- Already fixed by `<SERVER-XXXXX or commit SHA>` — transition ticket to `Won't Fix` / `Closed`
- Close as infra issue (`<evidence>`)

---

## AI Disclaimer

> This investigation was produced with AI assistance. Findings should be reviewed before
> acting on them. Commands marked **[CONFIRM BEFORE RUNNING]** require explicit user
> approval.

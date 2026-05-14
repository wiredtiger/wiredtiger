# WiredTiger Architecture Knowledge

Orientation hints accumulated from past BF investigations. Use these to form targeted
Explore questions — not as ground truth. Architecture changes; entries may be stale.
Always verify with source before relying on anything here.

## Adding entries

Add new entries after each investigation. Correct stale ones. Remove entries that are no longer true.

```
### <subsystem> — <concept or interaction>

<One to three sentences: who owns what, how components relate, what invariant holds.>

- **Learned from**: <WT-XXXXX>
- **Date**: <YYYY-MM-DD>
- **Confidence**: <High | Medium>
```

**Capture:** subsystem ownership, component interactions, data flow, system-level invariants.
**Never capture:** function names, struct names, field names, lock names, line numbers, config strings — read those from source.

Entries are grouped by subsystem. Keep each entry high-level enough that it remains true even if the implementation is refactored.

---

## btree / cursor

### layered cursor (disagg) — ingest vs. stable constituent ownership

In disaggregated storage, a layered cursor merges reads across an ingest layer and a stable layer. Iteration direction and which constituent is "current" must stay consistent. In leader mode the ingest layer is bypassed entirely after promotion because the ingest layer is expected to be empty after step-up.

- **Learned from**: WT-17454
- **Date**: 2026-05-12
- **Confidence**: High

### layered cursor (disagg) — iteration direction flags as position-recovery context

The layered cursor tracks iteration direction (forward/backward) via flags that persist across calls. These flags are not merely hints — they encode the context needed to recover or re-establish cursor position when a constituent cursor is in an unpositioned state. The flags must be set whenever the current constituent cursor is unpositioned; clearing them in that state breaks the invariant that the merge step can always determine which constituent is authoritative.

- **Learned from**: WT-17454
- **Date**: 2026-05-13
- **Confidence**: High

### layered cursor (disagg) — current-cursor pointer persistence and role-transition hazard

The layered cursor's "current" pointer (which constituent was authoritative at the end of the last iteration step) persists between successive cursor-next/prev calls and is used as the starting point for the next merge step. If the role of the node transitions between leader and follower between calls, the ingest constituent's validity changes — a previously valid current pointer may become invalid (e.g., pointing to a bypassed ingest cursor), violating the merge-step invariants and triggering assertion failures.

- **Learned from**: WT-17454
- **Date**: 2026-05-13
- **Confidence**: High

### layered cursor (disagg) — prepared-transaction interaction with iteration flags

When a prepared-transaction conflict is encountered during layered cursor iteration, the iteration direction flags are preserved so that the caller can retry the operation in the correct context. For all other errors the flags are cleared. This asymmetry means that prepared-transaction retries are safe, but any bug that causes a direction flag to be absent when a conflict occurs can surface as an assertion failure in the merge step.

- **Learned from**: WT-17454
- **Date**: 2026-05-13
- **Confidence**: High

---

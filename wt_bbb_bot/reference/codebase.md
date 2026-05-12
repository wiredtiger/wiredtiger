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
```

**Capture:** subsystem ownership, component interactions, data flow, system-level invariants.
**Never capture:** function names, struct names, field names, lock names, line numbers, config strings — read those from source.

Entries are grouped by subsystem. Keep each entry high-level enough that it remains true even if the implementation is refactored.

---

## btree / cursor

### layered cursor (disagg) — ingest vs. stable constituent ownership

In disaggregated storage, a layered cursor merges reads across an ingest layer and a stable layer. Iteration direction and which constituent is "current" must stay consistent. In leader mode the ingest layer is bypassed entirely after promotion.

- **Learned from**: WT-17454
- **Date**: 2026-05-12

---

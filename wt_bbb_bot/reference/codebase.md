# WiredTiger Architecture Knowledge

Orientation hints accumulated from past BF investigations. Use these to ask more targeted
Explore questions — not as ground truth. Architecture changes; entries may be stale.
Always verify with source before relying on anything here.

## Adding entries

Add new entries after each investigation. Correct stale ones. Remove entries that are no longer true.

```
### <subsystem> — <concept or interaction>

<One to three sentences: ownership, relationships, invariants.>

- **Learned from**: <WT-XXXXX>
- **Date**: <YYYY-MM-DD>
```

**Capture:** subsystem ownership, component interactions, data flow, system-level invariants.
**Skip:** function signatures, struct fields, lock names, line numbers — read those from source.

Entries are grouped by subsystem.

---

## btree / cursor

### layered cursor (disagg) — ingest + stable constituent ownership

`WT_CURSOR_LAYERED` wraps an ingest cursor and a stable cursor. Iteration merges both; the direction flag (`WT_CLAYERED_ITERATE_NEXT/PREV`) and `current_cursor` must be kept consistent. In leader mode the ingest cursor is skipped entirely (ingest is expected to be empty post-promotion).

- **Learned from**: WT-17454
- **Date**: 2026-05-12

---


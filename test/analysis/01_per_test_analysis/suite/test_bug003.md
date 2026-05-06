# test_bug003 — Bulk-load cursor open is not blocked by prior checkpoints

**File:** `test/suite/test_bug003.py`
**Storage mode:** General
**Components under test:** bulk cursor, checkpoint

## Test Cases

### `test_bug003.test_bug003`
- **What it tests:** Creates a table, optionally takes a named or unnamed checkpoint, then opens a `bulk` cursor. Verifies this does not raise an error — a regression test confirming that prior checkpoints do not prevent opening a bulk-load cursor on an empty table.
- **Components:** `src/cursor/cur_bulk.c`, `src/checkpoint/checkpoint.c`
- **Notes:** Parametrized across (file, table) URIs × (checkpoint, no checkpoint).

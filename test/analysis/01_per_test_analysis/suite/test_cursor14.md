# test_cursor14 — No 64K cursor limit (open 66,000 cursors on same data source)

**File:** `test/suite/test_cursor14.py`
**Storage mode:** General
**Components under test:** cursor open limits, session management

## Test Cases

### `test_cursor14.test_cursor14`
- **What it tests:** Opens 66,000 cursors on the same URI in a single session, verifying there is no 64K cursor-count limit enforced by WiredTiger.
- **Components:** `src/cursor/cur_std.c`, `src/session/`
- **Notes:** Scenarios: file-r, file-S, table-r, table-S, table-r-complex, table-S-complex. All cursors are opened without closing; the test verifies no error is returned at any count up to 66,000.

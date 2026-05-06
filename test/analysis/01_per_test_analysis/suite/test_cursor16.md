# test_cursor16 — Cached cursor count goes to zero after all sessions close

**File:** `test/suite/test_cursor16.py`
**Storage mode:** General
**Components under test:** cursor caching, session close, cursor sweep, in-memory tables

## Test Cases

### `test_cursor16.test_cursor16`
- **What it tests:** Creates 100 URIs, opens 100 sessions each holding cursors to all 100 URIs, then closes all sessions. Verifies that the `cursor_cached_count` statistic reaches 0 after all sessions are closed (no leaked cached cursors).
- **Components:** `src/cursor/cur_std.c`, `src/session/session_api.c`, `src/conn/conn_sweep.c`
- **Notes:** Scenarios: row (`key_format=S`) and var (`key_format=r`). Uses `in_memory=true` tables. Tests that cursor caching infrastructure cleans up correctly on session close.

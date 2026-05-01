# test_layered73 — Cursor key state preservation after WT_PREPARE_CONFLICT on layered table

**File:** `test/suite/test_layered73.py`
**Storage mode:** Disagg/Layered
**Components under test:** Prepared conflict handling, cursor position after `WT_PREPARE_CONFLICT`, `search_near`, `next`, `prev`

## Test Cases

### `test_layered73.test_search_near_key_preserved_on_prepare_conflict`
- **What it tests:** Commits keys 1, 3, 5 (ts=20), then prepares an update on key=2 (prepare_ts=50) in a separate session. Opens a cursor in a transaction at read_timestamp=60. Sets cursor key to 2 and calls `search_near()`; verifies it raises `WiredTigerError` (WT_PREPARE_CONFLICT) and that `cursor.get_key()` returns 2 (position preserved). If `commit=True`, commits the prepare and re-calls `search_near()`, verifying it returns 0 (found) with key=2 and value="prepared_value". If `commit=False`, rolls back the prepare (no further assertion).
- **Components:** `src/cursor/cur_layered.c`, `src/btree/bt_cursor.c` (prepare conflict handling)
- **Notes:** Parametrized by resolve (commit/rollback). Tests that `search_near` preserves the set key after a prepare conflict so a retry loop can re-try without re-setting the key. Node starts as follower, stepped up implicitly.

### `test_layered73.test_next_key_preserved_on_prepare_conflict`
- **What it tests:** Commits keys 1, 3, 5, prepares key=2. Positions cursor at key=1 via `search()`, then calls `next()`; verifies it raises `WiredTigerError`. Verifies `cursor.get_key()` raises with "requires key be set" (cursor is in an unpositioned state after the conflict). Calls `prev()` and verifies cursor lands back at key=1. If `commit=True`, commits the prepare and verifies `next()` returns key=2 with value="prepared_value".
- **Components:** `src/cursor/cur_layered.c`, `next` conflict path
- **Notes:** Tests that after a `next()` prepare conflict, calling `prev()` recovers position correctly. The "key state preserved" in the test name refers to the ability to recover via `prev()` to the previously-valid position.

### `test_layered73.test_prev_key_preserved_on_prepare_conflict`
- **What it tests:** Commits keys 1, 3, 5, prepares key=4 (between 3 and 5). Positions cursor at key=5 via `search()`, then calls `prev()`; verifies it raises `WiredTigerError`. Verifies `cursor.get_key()` raises with "requires key be set". Calls `next()` and verifies cursor lands at key=5. If `commit=True`, commits the prepare and verifies `prev()` returns key=4 with value="prepared_value".
- **Components:** `src/cursor/cur_layered.c`, `prev` conflict path
- **Notes:** Same pattern as `test_next_key_preserved_on_prepare_conflict` but in reverse direction. `layered:` URI, `preserve_prepared=true`.

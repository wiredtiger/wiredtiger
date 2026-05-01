# test_cursor_bound17 — Cursor bounds persistence across checkpoint, transaction commit/rollback, session.reset

**File:** `test/suite/test_cursor_bound17.py`
**Storage mode:** General
**Components under test:** cursor bound API, bound persistence, checkpoint, transaction lifecycle, session.reset

## Test Cases

### `test_cursor_bound17.test_bound_checkpoint_or_rollback`
- **What it tests:** Sets bounds on a cursor then performs each of the following operations and verifies bounds are still active (or correctly cleared) afterwards: (1) `session.checkpoint()` — bounds persist; (2) `session.rollback_transaction()` — bounds persist; (3) `session.commit_transaction()` — bounds persist; (4) `session.reconfigure()` — bounds persist; (5) `session.reset_cursors()` — bounds are cleared.
- **Components:** `src/cursor/cur_bound.c`, `src/checkpoint/`, `src/txn/`, `src/session/session_api.c`
- **Notes:** Scenarios: evict × file/table/colgroup × 7 key formats × 2 value formats. Eviction exercises the code path where bounds are validated against on-disk data after checkpoint.

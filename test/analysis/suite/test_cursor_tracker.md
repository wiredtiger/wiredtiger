# test_cursor_tracker — TestCursorTracker: shadow data structure for cursor operation verification

**File:** `test/suite/test_cursor_tracker.py`
**Storage mode:** General
**Components under test:** (utility/framework — not a runnable test class itself)

## Overview

`TestCursorTracker` is a base class (inheriting from `wttest.WiredTigerTestCase`) that maintains a parallel in-memory shadow of a WiredTiger table. It is used by `test_cursor02` and `test_cursor03` (and similar tests) to verify cursor operations against expected state.

Key design:
- Keys and values are generated as pure functions of `(major, minor, version)` triples encoded as 64-bit integers.
- `bitlist` is a sorted list of active key bits; `vers` dict maps bits to current version.
- `DELETED = 0xffffffffffffffff` sentinel for deleted column-store entries.
- Initial conditions populate the table with `npairs` entries, then close/reopen the connection to flush to disk (ensures the test exercises both skip-list and on-disk data paths).

## Methods (not test methods — framework utilities)

### `cur_initial_conditions(tablename, npairs, tablekind, keysizes, valuesizes)`
- **What it does:** Populates the table with N K/V pairs (major numbers 0..N-1), closes and reopens the connection to flush to disk.

### `cur_insert(cursor, major, minor)` / `cur_remove_here(cursor)` / `cur_search(cursor, major, minor)` / `cur_recno_search(cursor, recno)`
- **What it does:** Performs the corresponding cursor operation and updates the shadow state.

### `cur_next(cursor)` / `cur_previous(cursor)` / `cur_first(cursor)` / `cur_last(cursor)`
- **What it does:** Advances the cursor and updates the shadow position tracker.

### `cur_check_here(cursor)`
- **What it does:** Verifies the cursor's current key and value match the shadow state.

### `cur_check_forward(cursor, n)` / `cur_check_backward(cursor, n)`
- **What it does:** Iterates forward/backward n steps, checking each position.

### `encode_key_row` / `decode_key_row` / `encode_key_col` / `encode_value_row_or_col`
- **What it does:** Deterministic key/value encoding functions supporting variable sizes via SHA-224 hash stretching.

- **Notes:** `TRACE_API = False` can be enabled for debugging to print each WT API call. No standalone test methods; this file only defines the framework class.

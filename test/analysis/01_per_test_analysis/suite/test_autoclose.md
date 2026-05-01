# test_autoclose — Closed handle usage raises catchable errors and subordinate handles auto-close

**File:** `test/suite/test_autoclose.py`
**Storage mode:** General
**Components under test:** cursor lifecycle, session lifecycle, connection lifecycle, SWIG Python bindings

## Test Cases

### `test_autoclose.test_close_cursor1`
- **What it tests:** Uses a cursor after it is explicitly closed. Expects a `TypeError`/`RuntimeError` with the message `/wt_cursor.* is None/`.
- **Components:** `src/cursor/cur_std.c`, Python SWIG layer
- **Notes:** Platform-dependent exception type: `TypeError` on macOS, `RuntimeError` on Linux.

### `test_autoclose.test_close_cursor2`
- **What it tests:** Uses a cursor after its owning session is closed. Expects the same null-cursor error.
- **Components:** `src/session/session_api.c`, Python SWIG layer

### `test_autoclose.test_close_cursor3`
- **What it tests:** Uses a cursor after the entire connection is closed. Expects the same null-cursor error.
- **Components:** `src/conn/conn_api.c`, Python SWIG layer

### `test_autoclose.test_close_cursor4`
- **What it tests:** Confirms that `session.truncate()` with both cursor args null does not require null checking (it accepts null cursors by design). Opens a duplicate cursor via `open_cursor(None, inscursor, None)`, performs a ranged truncate, then a full truncate.
- **Components:** `src/session/session_api.c`, `src/btree/bt_delete.c`

### `test_autoclose.test_close_cursor5`
- **What it tests:** Verifies `cursor.compare()` correctly rejects a closed (None) cursor argument with `TypeError`, and rejects an explicitly-None cursor with the `/wt_cursor.* is None/` message.
- **Components:** `src/cursor/cur_std.c`, Python SWIG layer

### `test_autoclose.test_close_session1`
- **What it tests:** Uses a session after it is explicitly closed. Expects `/wt_session.* is None/`.
- **Components:** `src/session/session_api.c`, Python SWIG layer

### `test_autoclose.test_close_session2`
- **What it tests:** Uses a session after the connection is closed. Expects `/wt_session.* is None/`.
- **Components:** `src/conn/conn_api.c`, Python SWIG layer

### `test_autoclose.test_close_connection1`
- **What it tests:** Uses a connection after it is closed. Expects `/connection is closed/` from the TestSuiteConnection wrapper (not the SWIG layer).
- **Components:** `src/conn/conn_api.c`, test infrastructure (TestSuiteConnection)

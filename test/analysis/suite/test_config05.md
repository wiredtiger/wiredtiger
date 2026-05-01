# test_config05 — Multiple connection opens, session_max, exclusive flag

**File:** `test/suite/test_config05.py`
**Storage mode:** General
**Components under test:** connection API, session management, exclusive connection flag

## Test Cases

### `test_config05.test_one`
- **What it tests:** Opening a single connection to the same home succeeds.
- **Components:** `src/conn/conn_open.c`

### `test_config05.test_one_session`
- **What it tests:** A single session can be opened on an existing connection.
- **Components:** `src/session/`

### `test_config05.test_too_many_sessions`
- **What it tests:** Opening more sessions than `session_max` causes an error.
- **Components:** `src/session/`
- **Notes:** Skipped for tiered hook.

### `test_config05.test_exclusive_create`
- **What it tests:** `exclusive` flag prevents a second connection from opening to same home directory.
- **Components:** `src/conn/conn_open.c`

### `test_config05.test_multi_create`
- **What it tests:** Multiple non-exclusive connections to the same home are allowed.
- **Components:** `src/conn/conn_open.c`

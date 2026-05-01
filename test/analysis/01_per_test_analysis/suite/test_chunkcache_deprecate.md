# test_chunkcache_deprecate — Chunk cache deprecation error and warning handling

**File:** `test/suite/test_chunkcache_deprecate.py`
**Storage mode:** General
**Components under test:** chunk cache configuration, deprecation handling, connection open

## Test Cases

### `test_chunkcache07.test_chunk_cache_enabled_error`
- **What it tests:** Verifies that opening a connection with `chunk_cache=(enabled=true)` raises `WiredTigerError` with the message "chunk cache has been deprecated and is no longer supported".
- **Components:** `src/conn/conn_open.c`, `src/config/`
- **Notes:** Calls `wiredtiger_open('.', 'create,chunk_cache=(enabled=true)')` inside `assertRaisesWithMessage`. Reopens the connection normally in tearDown so the test framework can close cleanly.

### `test_chunkcache07.test_chunk_cache_disabled_warning`
- **What it tests:** Verifies that opening a connection with `chunk_cache=(enabled=false)` emits a deprecation warning to stdout rather than raising an error.
- **Components:** `src/conn/conn_open.c`, `src/config/`
- **Notes:** Uses `expectedStdoutPattern('chunk cache has been deprecated and is no longer supported')` to assert the warning is printed. Tests that disabled chunk cache is tolerated but warned about.

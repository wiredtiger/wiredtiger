# test_compact08 — Compaction disallowed for in-memory and read-only databases

**File:** `test/suite/test_compact08.py`
**Storage mode:** General
**Components under test:** compaction subsystem, in-memory mode, read-only mode, API validation

## Test Cases

### `test_compact08.test_compact08`
- **What it tests:** Verifies that both foreground and background compaction are correctly rejected when the database is opened in in-memory or read-only mode, with appropriate error messages.
- **Components:** `src/session/session_compact.c`, `src/support/background_compact.c`
- **Notes:** In-memory mode: foreground compact prints "Compact does not work for in-memory databases" (no exception); background compact raises `WiredTigerError` with "Background compact cannot be configured for in-memory or readonly databases". Read-only mode: both foreground and background raise `WiredTigerError` with "Operation not supported". Uses `reopen_conn(config='in_memory=true')` and `reopen_conn(config='readonly=true')`.

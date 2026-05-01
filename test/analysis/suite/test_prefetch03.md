# test_prefetch03 — Prefetch incompatibility with in-memory mode

**File:** `test/suite/test_prefetch03.py`
**Storage mode:** General (in-memory)
**Components under test:** prefetch configuration, in-memory mode

## Test Cases

### `test_prefetch03.test_prefetch03`
- **What it tests:** Verifies that opening a WiredTiger connection with both `in_memory=true` and `prefetch=(available=true)` emits a warning message indicating the combination is unsupported; prefetch is a read-ahead mechanism that is meaningless in-memory
- **Components:** `conn/conn_prefetch.c`, `conn/conn_open.c`
- **Notes:** Expects a specific stdout warning pattern (`/Pre-fetch/` or similar); does not assert an error — the connection opens successfully, but the warning flags the misconfiguration; uses `wiredtiger_open()` directly with combined options

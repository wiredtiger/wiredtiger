# test_reconfig04 — Session reconfiguration: isolation level and ignore_cache_size

**File:** `test/suite/test_reconfig04.py`
**Storage mode:** General
**Components under test:** session reconfiguration API, isolation levels, cache management

## Test Cases

### `test_reconfig04.test_session_reconfigure`
- **What it tests:** Verifies that `session.reconfigure()` can change per-session settings at runtime: `ignore_cache_size` (true/false) and `isolation` level (snapshot, read-committed, read-uncommitted); verifies all valid combinations succeed without error
- **Components:** `session/session_api.c`, `txn/txn.c`
- **Notes:** No scenarios; tagged with `[TEST_TAGS] session_api:reconfigure`; tests the full isolation level sequence (snapshot → read-committed → read-uncommitted → back to snapshot); `ignore_cache_size=true` allows a session to bypass eviction pressure checks; all reconfigure calls are between transactions (no open transaction when reconfiguring)

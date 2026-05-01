# test_isolation01 — Transaction isolation levels and reset_snapshot API behavior

**File:** `test/suite/test_isolation01.py`
**Storage mode:** General
**Components under test:** transaction isolation, `session.reset_snapshot`, error handling

## Test Cases

### `test_isolation01.test_isolation_level`
- **What it tests:** Verifies behavior of write operations and the `reset_snapshot` API under all three isolation levels: `read-uncommitted`, `read-committed`, and `snapshot`.
- **Components:** `src/txn/txn_api.c`, `src/txn/txn.c`, `src/session/session_api.c`
- **Notes:** Parameterized by isolation level:
  - `read-uncommitted` — write (insert) must fail: `"not supported in read-committed or read-uncommitted transactions"`. `reset_snapshot` must fail: `"not supported in read-committed or read-uncommitted transactions"`.
  - `read-committed` — same restrictions as read-uncommitted.
  - `snapshot` — insert succeeds. `reset_snapshot` after a write fails: `"only supported before .* modifications"`. After starting a new transaction, `reset_snapshot` without prior writes succeeds.

  Two transaction phases tested:
  1. Transaction with write attempt → check write error and reset_snapshot error.
  2. Transaction with read-only search → `reset_snapshot` succeeds for snapshot, fails for others.

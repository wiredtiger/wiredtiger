# test_timestamp01 — Timestamp value range validation

**File:** `test/suite/test_timestamp01.py`
**Storage mode:** General
**Components under test:** transaction timestamp API, `timestamp_transaction`, `commit_transaction`

## Test Cases

### `test_timestamp01.test_timestamp_range`
- **What it tests:** Validates boundary conditions for commit timestamps: zero not permitted, too-large (1<<5000) not permitted, negative hex string fails with parse error, invalid characters (`/`, `` ` ``, `{`) fail with parse error, timestamp=1 succeeds, upper-case hex succeeds, max 64-bit timestamp (1<<64-1) succeeds. Also confirms that `timestamp_transaction` outside a running transaction raises an error.
- **Components:** `txn_timestamp.c`, `config.c`
- **Notes:** Tests hex parsing edge cases; timestamp=0 and impossibly large values are the primary guard conditions.

# test_timestamp27 — Rollback timestamp API for prepared transactions

**File:** `test/suite/test_timestamp27.py`
**Storage mode:** General
**Components under test:** `rollback_timestamp` in `timestamp_transaction`, `preserve_prepared` config

## Test Cases

### `test_timestamp27_preserve_prepared_off.test_non_prepared`
- **What it tests:** Attempts to set `rollback_timestamp` on a non-prepared transaction — expects error `'rollback timestamp is set for an non-prepared transaction'`.
- **Components:** `txn_timestamp.c`, `txn.c`

### `test_timestamp27_preserve_prepared_off.test_prepared`
- **What it tests:** Prepares a transaction and sets `rollback_timestamp` via `timestamp_transaction` — succeeds (no error).
- **Components:** `txn_timestamp.c`, `txn_prepare.c`

### `test_timestamp27_preserve_prepared_on.test_non_prepared`
- **What it tests:** With `preserve_prepared=true`, confirms `rollback_timestamp` on non-prepared txn still errors.
- **Components:** `txn_timestamp.c`

### `test_timestamp27_preserve_prepared_on.test_prepared`
- **What it tests:** Prepares with `prepared_id=123`; sets `rollback_timestamp` — succeeds.
- **Components:** `txn_timestamp.c`, `txn_prepare.c`

### `test_timestamp27_preserve_prepared_on.test_rollback_timestamp_lt_stable`
- **What it tests:** Prepared transaction; advances stable to 100; sets rollback_timestamp=90 (< stable) — expects `'is not newer than the stable timestamp'`.
- **Components:** `txn_timestamp.c`

### `test_timestamp27_preserve_prepared_on.test_rollback_timestamp_eq_stable`
- **What it tests:** Sets rollback_timestamp=100 (equal to stable=100) — expects same error (must be strictly newer).
- **Components:** `txn_timestamp.c`

### `test_timestamp27_preserve_prepared_on.test_rollback_timestamp_with_commit_timestamp`
- **What it tests:** Attempts to set both `rollback_timestamp` and `commit_timestamp` in one call (or after commit was set) — expects `'commit timestamp and rollback timestamp should not be set together'`.
- **Components:** `txn_timestamp.c`

### `test_timestamp27_preserve_prepared_on.test_roundup_prepare_timestamp`
- **What it tests:** With `preserve_prepared=true`, `roundup_timestamps=(prepare=true)` in `begin_transaction` is rejected with an error.
- **Components:** `txn_timestamp.c`
- **Notes:** Both test classes are parameterized over row/var key formats.

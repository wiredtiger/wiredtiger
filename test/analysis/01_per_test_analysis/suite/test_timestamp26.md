# test_timestamp26 — write_timestamp_usage and read_timestamp assert settings

**File:** `test/suite/test_timestamp26.py`
**Storage mode:** General
**Components under test:** `write_timestamp_usage`, `assert=(read_timestamp=...)`, `session.alter`, in-memory and logged timestamp ignore

## Test Cases

### `test_timestamp26_wtu_never.test_wtu_never`
- **What it tests:** Creates a table with `write_timestamp_usage=never`; attempts to commit with a timestamp — expects `'set when disallowed'` error; commits without timestamp — succeeds. Parameterized over whether timestamp is set at `timestamp_transaction` vs `commit_transaction`.
- **Components:** `txn_timestamp.c`, `schema.c`
- **Notes:** Non-diagnostic build only. Parameterized over row/var × with-ts/without-ts × at-commit/pre-set.

### `test_timestamp26_read_timestamp.test_read_timestamp`
- **What it tests:** Tests `assert=(read_timestamp=always/never/none)`: `always` requires a read timestamp on cursor operations (error if absent); `never` disallows read timestamps (error if present); `none` accepts either.
- **Components:** `txn_timestamp.c`, `btree.c`
- **Notes:** Non-diagnostic build. Parameterized over row/var × 3 assert values.

### `test_timestamp26_alter.test_alter`
- **What it tests:** Creates table with `never`, verifies writes with timestamp fail; `alter` to `ordered`; verifies timestamped writes succeed and non-timestamped writes are then rejected.
- **Components:** `schema.c`, `txn_timestamp.c`
- **Notes:** Non-diagnostic build. Parameterized over row/var.

### `test_timestamp26_alter_inconsistent_update.test_alter_inconsistent_update`
- **What it tests:** Creates mixed timestamp/no-timestamp data; alters to `write_timestamp_usage=ordered`; verifies that decreasing timestamps and missing timestamps are detected.
- **Components:** `schema.c`, `txn_timestamp.c`
- **Notes:** Non-diagnostic build. Must advance oldest_timestamp before alter to avoid EBUSY.

### `test_timestamp26_inconsistent_update.test_timestamp_inconsistent_update`
- **What it tests:** With `write_timestamp_usage=ordered`, verifies that a write at ts=1 after a write at ts=2 for the same key is rejected; a different key at ts=1 is allowed; within one transaction updating key1 (last ts=10) and key2 (last ts=15) at ts=13 fails (key2's ts is inconsistent).
- **Components:** `txn_timestamp.c`

### `test_timestamp26_inconsistent_update.test_timestamp_ts_then_nots`
- **What it tests:** With `write_timestamp_usage=ordered`, writes key at ts=20 then writes without timestamp — expects `'configured to always use timestamps once they are first used'`.
- **Components:** `txn_timestamp.c`

### `test_timestamp26_inconsistent_update.test_timestamp_ts_order`
- **What it tests:** Verifies that `write_timestamp_usage=ordered` allows setting timestamp before writes, between writes, or at commit — all succeed if timestamps are ordered.
- **Components:** `txn_timestamp.c`

### `test_timestamp26_log_ts.test_log_ts`
- **What it tests:** With connection logging enabled (which causes timestamps to be ignored), confirms that `write_timestamp_usage=always/never` config does not cause errors when committing with or without timestamps in logged tables.
- **Components:** `log.c`, `txn_timestamp.c`
- **Notes:** Skipped for disagg with `write_timestamp_usage=never`.

### `test_timestamp26_in_memory_ts.test_in_memory_ts`
- **What it tests:** Tests that in-memory and logged connections ignore timestamp violations by default, but object-level `log=(enabled=false)` can override to enforce them.
- **Components:** `txn_timestamp.c`, `conn.c`
- **Notes:** Non-diagnostic build. Parameterized over row/var × in-memory/logged conn × object log enabled/disabled.

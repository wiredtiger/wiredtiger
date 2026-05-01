# test_rollback_to_stable41 — RTS dryrun config applies only to a single call

**File:** `test/suite/test_rollback_to_stable41.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, dryrun mode

## Test Cases

### `test_rollback_to_stable41.test_rollback_to_stable`
- **What it tests:** Verifies that the `dryrun=true` config for `rollback_to_stable()` applies only to that single call and does not persist to the next call. Writes value_a@10 and value_b@30 to 1,000 rows. Sets stable=20. Calls RTS with `dryrun=true`: value_b still visible at ts=30. Calls RTS without dryrun: value_b is now gone, value_a visible at ts=30. The test confirms dryrun is a per-call option, not a persistent mode.
- **Components:** `src/txn/txn_rollback_to_stable.c`
- **Notes:** Parametrized on key_format (column/row_integer) and worker threads (0/4/8). No crash-restart. `verbose=(rts:5)`.

# test_recovery01 — Verify WiredTiger logs time spent during recovery and shutdown

**File:** `test/suite/test_recovery01.py`
**Storage mode:** General
**Components under test:** recovery, logging, rollback_to_stable, checkpoint

## Test Cases

### `test_recovery01.test_recovery`
- **What it tests:** Creates one logged and one non-logged table, writes data at multiple timestamps, sets stable_timestamp=10, checkpoints, then either crashes (simulate_crash_restart) or cleanly reopens. Verifies that after restart the logged table retains all updates (log-based recovery), while the non-logged table is rolled back to its stable version (ts=10, value a).
- **Components:** `src/log/`, `src/txn/txn_rollback_to_stable.c`, `src/checkpoint/`
- **Notes:** Parameterized on key_format (column `r` vs row-integer `i`) and restart type (crash vs clean shutdown). Stable timestamp is pinned at 1 initially then advanced to 10 before checkpoint. Post-recovery checks: logged table sees valueb (latest), non-logged table sees valuea at ts=10 and ts=20.

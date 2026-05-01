# test_checkpoint_snapshot02 — RTS after crash/backup under no-timestamp, timestamp, and txnid scenarios

**File:** `test/suite/test_checkpoint_snapshot02.py`
**Storage mode:** General
**Components under test:** checkpoint snapshot, rollback to stable, crash recovery, backup

## Test Cases

### `test_checkpoint_snapshot02.test_checkpoint_snapshot_with_txnid_and_ts`
- **What it tests:** Verifies RTS correctness after crash restart when the checkpoint snapshot contains both transaction IDs and timestamps. Uncommitted transactions in the snapshot must be rolled back; committed-and-stable data must survive.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/checkpoint/`
- **Notes:** Uses `timing_stress_for_test=[checkpoint_slow]` to create snapshot overlap. After crash restart, reads confirm the expected stable state.

### `test_checkpoint_snapshot02.test_checkpoint_snapshot_with_ts`
- **What it tests:** Same scenario but using only timestamps (no explicit txn IDs). Verifies RTS correctly handles the timestamp-only snapshot case.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/checkpoint/`
- **Notes:** Writes committed at timestamps within stable_ts survive; writes above stable_ts are rolled back after crash restart.

### `test_checkpoint_snapshot02.test_checkpoint_snapshot_without_ts_and_backup`
- **What it tests:** Verifies RTS correctness using a backup-based crash simulation (copy_wiredtiger_home) without timestamps. Tests the non-timestamped snapshot recovery path.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/checkpoint/`, `src/backup/`
- **Notes:** Uses backup copy rather than `simulate_crash_restart`. Non-timestamped transactions in the snapshot are rolled back; committed transactions are preserved.

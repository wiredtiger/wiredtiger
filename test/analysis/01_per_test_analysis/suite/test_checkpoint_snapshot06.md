# test_checkpoint_snapshot06 — Truncate+insert concurrent with checkpoint across two tables; crash/backup recovery

**File:** `test/suite/test_checkpoint_snapshot06.py`
**Storage mode:** General
**Components under test:** checkpoint snapshot, truncation, concurrent inserts, crash recovery, backup

## Test Cases

### `test_checkpoint_snapshot06.test_checkpoint_snapshot06`
- **What it tests:** Verifies cross-table atomicity when a truncation and an insertion operation on two separate tables run concurrently with a checkpoint. After crash restart or backup recovery, both tables must reflect a consistent state — either both the truncation and insert are visible or neither is.
- **Components:** `src/checkpoint/`, `src/btree/bt_delete.c`, `src/txn/txn_rollback_to_stable.c`, `src/backup/`
- **Notes:** Uses `timing_stress_for_test=[checkpoint_slow]` for concurrency overlap. Transaction modifies both tables (truncate on one, insert on another). Crash or backup recovery must produce consistent cross-table state. Tests the atomicity guarantee of checkpoint snapshots across multiple tables with mixed operation types.

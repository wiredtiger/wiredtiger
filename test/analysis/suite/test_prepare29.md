# test_prepare29 — Prepared tombstone evicted+checkpointed+crash-restart: no write conflict on remove

**File:** `test/suite/test_prepare29.py`
**Storage mode:** General (skipped for disagg)
**Components under test:** prepared transactions, tombstones, eviction, checkpoint, crash recovery, write conflict

## Test Cases

### `test_prepare29.test_prepare29`
- **What it tests:** Inserts a value, prepares a tombstone (delete), evicts the page, takes a checkpoint, then simulates a crash via `simulate_crash_restart`; after recovery, attempts to remove the same key at a new timestamp and verifies there is no spurious write conflict
- **Components:** `txn/txn_prepare.c`, `btree/bt_delete.c`, `evict/evict_page.c`, `checkpoint/checkpoint.c`, `conn/conn_recover.c`
- **Notes:** Scenarios: column/integer-row; skipped for disagg hook; the bug was that after crash recovery, a prepared tombstone that had been checkpointed to disk could leave stale state that caused a subsequent remove to return `WT_ROLLBACK` (write conflict) instead of succeeding; the test verifies that the remove at ts=201 succeeds after recovery

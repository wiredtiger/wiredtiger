# test_rollback_to_stable06 — RTS removes all keys when stable timestamp is before all commits

**File:** `test/suite/test_rollback_to_stable06.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, history store, transactions, checkpoint, eviction

## Test Cases

### `test_rollback_to_stable06.test_rollback_to_stable`
- **What it tests:** Verifies that when the stable timestamp (10) is earlier than all commit timestamps (20, 30, 40, 50), RTS removes all 1,000 keys from the table. After RTS checks show 0 rows at each timestamp. Then re-inserts the same data at the same timestamps and checkpoints to confirm HS is clean (no duplicate-key conflict). Stats: `calls=1`, `keys_restored=0`; `upd_aborted + hs_removed + keys_removed >= nrows*4` (non-memory); `upd_aborted + keys_removed == nrows*4` (in-memory).
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/history/`, `src/checkpoint/`
- **Notes:** Parametrized on key_format (column/row_integer), in_memory, prepare, evict (optional post-reinsert eviction), worker threads (0/4/8). Oldest+stable pinned to ts=10 before first writes. In-memory path uses `log=(enabled=false)`. `cache_size=50MB`.

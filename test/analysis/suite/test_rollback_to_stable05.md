# test_rollback_to_stable05 — RTS cleans history store for non-timestamp tables

**File:** `test/suite/test_rollback_to_stable05.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, history store, non-timestamp updates, checkpoint

## Test Cases

### `test_rollback_to_stable05.test_rollback_to_stable`
- **What it tests:** Verifies that RTS correctly handles two non-timestamp tables (all updates committed at ts=0). A long-running transaction is opened in a second session to keep old history alive while 3 updates are written to each table. After checkpoint and closing the long-running txn, RTS is called. Verifies that `valued` (latest) remains visible and no keys are removed or restored. Stats: `calls=1`, `keys_removed=0`, `keys_restored=0`; in non-in-memory mode `hs_removed >= 0`.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/history/`, `src/checkpoint/`
- **Notes:** Parametrized on key_format (column/row_integer), in_memory, prepare, worker threads (0/4/8). Two URIs: `rollback_to_stable05_1` and `rollback_to_stable05_2`. Updates written with ts=0 (non-timestamp). In-memory path: `upd_aborted=0`, `hs_removed=0`. Non-memory path: `hs_removed >= 0`. `cache_size=50MB`.

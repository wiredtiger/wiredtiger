# test_prepare40 — Checkpoint after RESOLVE_PREPARE_ON_DISK rollback does not crash

**File:** `test/suite/test_prepare40.py`
**Storage mode:** General (`precise_checkpoint=true,preserve_prepared=true`)
**Components under test:** prepared transactions, RESOLVE_PREPARE_ON_DISK, eviction, rollback, checkpoint, reconciliation stats

## Test Cases

### `test_prepare40.test_prepare40`
- **What it tests:** Tests that checkpointing after writing prepared updates to disk via the RESOLVE_PREPARE_ON_DISK eviction path, then rolling back with a rollback_timestamp > stable_timestamp, then checkpointing again does not crash or assert; verifies that `rec_time_window_prepared` stat is correctly set during the intermediate checkpoint
- **Components:** `txn/txn_prepare.c`, `evict/evict_page.c`, `btree/bt_rec.c`, `checkpoint/checkpoint.c`
- **Notes:** `conn_config = 'precise_checkpoint=true,preserve_prepared=true'`; no scenarios; the RESOLVE_PREPARE_ON_DISK path is taken when eviction encounters a prepared update on a page that has already been written to disk; the rollback with a future rollback_ts leaves the prepared update in a partially-resolved state on disk; the second checkpoint must correctly handle this state without crashing

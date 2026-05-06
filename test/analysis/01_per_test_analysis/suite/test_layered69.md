# test_layered69 — Prepared rollback reconciliation with disagg storage

**File:** `test/suite/test_layered69.py`
**Storage mode:** Disagg/Layered
**Components under test:** Prepared transaction reconciliation, page write stats after rollback, page delta vs full-image paths

## Test Cases

### `test_layered69.test_rollback_prepared_update`
- **What it tests:** Inserts committed values for keys 1–19 (ts=21, stable=21), optionally evicts pages, then prepares an update for key=19 (prepare_ts=35, prepared_id=1) and immediately rolls it back (rollback_ts=45). Verifies checkpoint stats through four phases: (1) stable=21: `rec_time_window_prepared=False`; (2) stable=35: `rec_time_window_prepared=True` (prepare within stable window), page written; (3) stable=45: `rec_time_window_prepared=False`, committed value written; (4) reads key=19 and verifies value is 'commit_value'. The page write stat checked is `rec_page_delta_leaf` (delta enabled) or `rec_page_full_image_leaf` (delta disabled).
- **Components:** `src/btree/bt_rec.c`, `src/conn/conn_ckpt.c`, prepared transaction reconciliation
- **Notes:** Extends `test_prepare_preserve_prepare_base`. Parametrized by evict (True/False) × delta (enabled/disabled). `table:` URI with `type=layered`. The `checkpoint_and_verify_stats()` helper verifies that the listed stats are True (non-zero) or False (zero) after each checkpoint.

### `test_layered69.test_rollback_prepared_reinsert`
- **What it tests:** Inserts keys 1–19 (ts=21), then deletes key=19 (ts=22, oldest=22, stable=30 making delete globally visible), optionally evicts, then prepares a re-insert of key=19 (prepare_ts=35) and rolls it back (rollback_ts=45). Verifies the same four-phase stat progression as `test_rollback_prepared_update`. Final verify: `cursor.search(key=19) == WT_NOTFOUND` (the committed state is deletion).
- **Components:** `src/btree/bt_rec.c`, prepared transaction reconciliation after tombstone
- **Notes:** Tests the re-insert path: prepared insert on top of a deleted (globally visible) key.

### `test_layered69.test_rollback_prepared_remove`
- **What it tests:** Inserts keys 1–19 (ts=21, stable=21), optionally evicts, then prepares a removal of key=19 (prepare_ts=35) and rolls it back (rollback_ts=45). Verifies the same four-phase stat progression. Final verify: `cursor[19] == 'commit_value'` (the committed state before the prepared remove is restored).
- **Components:** `src/btree/bt_rec.c`, prepared transaction reconciliation for tombstone
- **Notes:** Tests the prepared-remove path: rollback must restore the original committed value and correctly update `rec_time_window_prepared` across all checkpoint phases.

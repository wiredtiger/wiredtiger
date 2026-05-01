# test_rollback_to_stable38 — RTS fast truncates entire history store btree

**File:** `test/suite/test_rollback_to_stable38.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, history store, fast-truncate, crash recovery

## Test Cases

### `test_rollback_to_stable38.test_rollback_to_stable38`
- **What it tests:** Verifies that RTS can fast-truncate the entire history store btree when appropriate. Creates 1,000,000 rows. Pins a second session in a transaction (to keep old history). Writes two rounds of value_a to all rows. Checkpoints. Second session commits and closes. Crash-restart. Post-restart: verifies `cache_hs_btree_truncate > 0` (history store was fast-truncated) and `rec_page_delete_fast > 0`.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/history/`, `src/truncate/`, `src/checkpoint/`
- **Notes:** Skipped for TSan (`TESTUTIL_TSAN=1`) as 1M rows causes "Cache stuck for too long" under sanitizers. Parametrized on key_format (column/row_integer). Uses RTS verifier as teardown. `cache_size=50MB`. The long-running transaction in session2 forces history to be retained in HS, then RTS can bulk-truncate the HS btree.

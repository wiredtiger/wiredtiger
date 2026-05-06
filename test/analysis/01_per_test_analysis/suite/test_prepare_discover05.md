# test_prepare_discover05 — prepared_discover cursor with prepared delete and additional eviction+checkpoint step

**File:** `test/suite/test_prepare_discover05.py`
**Storage mode:** General (`precise_checkpoint=true,preserve_prepared=true`)
**Components under test:** prepared transactions, prepared_discover cursor, tombstones, eviction, claim_prepared_id, commit/rollback

## Test Cases

### `test_prepare_discover05.test_prepare_discover04`
- **What it tests:** Extends the test_prepare_discover04 scenario with an additional eviction and checkpoint step after the claim+commit/rollback; verifies that the sequence of discover → claim → resolve → evict → checkpoint completes without crash or data corruption for both prepared delete commit and rollback paths
- **Components:** `txn/txn_prepare.c`, `btree/bt_delete.c`, `cursor/cur_prepare_discover.c`, `evict/evict_page.c`, `backup/backup.c`, `checkpoint/checkpoint.c`
- **Notes:** Scenarios: commit/rollback; the file content is structured identically to test_prepare_discover04 but includes the additional eviction step after resolution; this exercises the code path where a claimed+resolved prepared transaction's pages are evicted before the final checkpoint, verifying no stale prepared state remains on the evicted pages

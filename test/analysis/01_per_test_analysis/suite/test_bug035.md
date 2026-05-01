# test_bug035 — WT-13716: selective backup removes fast-truncated HS pages correctly

**File:** `test/suite/test_bug035.py`
**Storage mode:** General
**Components under test:** selective backup, fast truncate, history store, RTS, metadata verification

## Test Cases

### `test_bug035.test_bug035`
- **What it tests:** Validates the fix for WT-13716 where fast-truncated history store pages belonging to excluded tables could reappear in a selective backup after a shutdown. Creates 10 tables (`uri_1`–`uri_10`), inserts 1000 records in each at timestamps 1–9, sets `stable_timestamp=15`, and checkpoints to persist data in the history store. Takes a selective backup of the first 5 tables (explicitly excluding the last 5). Opens the backup directory with `backup_restore_target=[uri_1,...,uri_5]`, which internally runs RTS to truncate HS pages belonging to excluded tables. Asserts that `rec_page_delete_fast` statistic is greater than 0 (confirming fast truncate ran). Reopens the backup directory with `verify_metadata=true` to confirm the excluded 5 tables are absent from both the history store and metadata.
- **Components:** `src/backup/backup.c`, `src/history/hs_cursor.c`, `src/txn/txn_rollback.c`, `src/meta/meta_api.c`
- **Notes:** Non-parametrized. 1 GB cache. Uses `backup_base` helper (`take_selective_backup`). 10 URIs; 9 × 1000 inserts per table.

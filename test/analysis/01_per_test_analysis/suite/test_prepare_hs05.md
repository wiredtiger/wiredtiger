# test_prepare_hs05 — Prepared update rollback restores history store value correctly

**File:** `test/suite/test_prepare_hs05.py`
**Storage mode:** General
**Components under test:** prepared transactions, history store, rollback, eviction, checkpoint, visibility

## Test Cases

### `test_prepare_hs05.test_check_prepare_abort_hs_restore`
- **What it tests:** Inserts value1 at ts=2, updates+removes with value2 at ts=3 (creating a tombstone), then prepares value3 at ts=4; evicts the page using `debug=(release_evict)` cursor with `ignore_prepare=true`; rolls back the prepared transaction; takes a checkpoint; verifies that value1 is readable from the history store at ts=2 and that the key returns WT_NOTFOUND at the latest timestamp (because the tombstone from ts=3 is the last committed entry)
- **Components:** `txn/txn_prepare.c`, `txn/txn_rollback.c`, `history/hs_cursor.c`, `evict/evict_page.c`, `checkpoint/checkpoint.c`
- **Notes:** Scenarios: column/integer-row; the sequence (insert → update+remove → prepare → evict with ignore_prepare → rollback) specifically tests that HS restore after a prepare rollback correctly handles the case where both value2 and the tombstone at ts=3 were moved to HS by eviction; after rollback the HS content should allow reading value1 at ts=2 but return WT_NOTFOUND at ts=4+

# test_prepare15 — History store interaction with prepared update+remove

**File:** `test/suite/test_prepare15.py`
**Storage mode:** General and in-memory
**Components under test:** prepared transactions, history store, eviction, timestamps, commit/rollback

## Test Cases

### `test_prepare15.test_prepare_hs_update_and_tombstone`
- **What it tests:** Inserts an initial value, then in a prepared transaction performs an update followed by a tombstone on the same key; eviction pressure moves the prior value to the history store; verifies that after commit or rollback the value at the pre-prepare timestamp is readable from the history store
- **Components:** `txn/txn_prepare.c`, `history/hs_cursor.c`, `evict/evict_page.c`, `btree/bt_delete.c`
- **Notes:** Scenarios: no_inmem/inmem × column/integer-row × commit/rollback

### `test_prepare15.test_prepare_hs_update`
- **What it tests:** Same as above but the prepared transaction only performs an update (no tombstone); prior value is moved to HS by eviction; verifies value at pre-prepare timestamp is accessible from HS after commit/rollback
- **Components:** `txn/txn_prepare.c`, `history/hs_cursor.c`, `evict/evict_page.c`
- **Notes:** Scenarios: no_inmem/inmem × column/integer-row × commit/rollback

### `test_prepare15.test_prepare_no_hs`
- **What it tests:** Verifies the same prepare+commit/rollback scenarios when no eviction to history store has occurred; value remains in the cache chain throughout; confirms correct visibility without HS involvement
- **Components:** `txn/txn_prepare.c`, `btree/bt_cursor.c`
- **Notes:** Scenarios: no_inmem/inmem × column/integer-row × commit/rollback

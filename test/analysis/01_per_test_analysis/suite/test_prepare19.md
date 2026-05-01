# test_prepare19 — Aborted prepared update at tail of long update chain: no write conflict on subsequent insert

**File:** `test/suite/test_prepare19.py`
**Storage mode:** In-memory only
**Components under test:** prepared transactions, update chain, rollback, tombstone, write conflict

## Test Cases

### `test_prepare19.test_server_example`
- **What it tests:** Reproduces a scenario where over 1,000 aborted (rolled-back) updates accumulate on an update chain for a key; then a prepare+evict+rollback cycle is executed; verifies that a subsequent insert on the same key does not incorrectly return a write conflict (which would happen if the tombstone from the aborted prepare was not correctly appended to the chain)
- **Components:** `txn/txn_prepare.c`, `txn/txn_rollback.c`, `btree/bt_update.c`, `evict/evict_page.c`
- **Notes:** In-memory configuration only (`in_memory=true`); the long chain of aborted updates creates a specific internal state that triggers the bug; guards against the regression where rollback of a prepare on a key with many prior aborted updates would fail to correctly mark the tombstone, causing subsequent inserts to see a spurious write conflict

# test_prepare22 — Prepare evict+checkpoint+rollback+rollback_to_stable+evict again

**File:** `test/suite/test_prepare22.py`
**Storage mode:** General
**Components under test:** prepared transactions, eviction, checkpoint, rollback, rollback_to_stable, history store

## Test Cases

### `test_prepare22.test_prepare22`
- **What it tests:** Full lifecycle test: insert value_a, prepare value_b, evict page, checkpoint HS, rollback prepare, then run rollback_to_stable, then evict the page again; verifies that values at all relevant timestamps are correct at each stage
- **Components:** `txn/txn_prepare.c`, `txn/txn_rollback.c`, `evict/evict_page.c`, `checkpoint/checkpoint.c`, `rts/rts.c`, `history/hs_cursor.c`
- **Notes:** Scenarios: column/integer-row × delete/non-delete (whether the prepared transaction is an update or a tombstone); the multi-step eviction+checkpoint+rollback+RTS sequence ensures correct cleanup at each step; after RTS, stable_timestamp determines what remains visible; a second eviction verifies the page is correctly cleaned up after RTS traversal

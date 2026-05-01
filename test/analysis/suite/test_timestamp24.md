# test_timestamp24 — Conflicting update rejected after eviction with aborted update

**File:** `test/suite/test_timestamp24.py`
**Storage mode:** General
**Components under test:** eviction, write conflict, history store, aborted transactions

## Test Cases

### `test_timestamp24.test_timestamp`
- **What it tests:** Session1 writes value_a at ts=20; starts a new transaction at read_ts=25 and reads value_a (leaves transaction open); session2 writes value_b at ts=50; evicts the page; session2 attempts value_c but aborts; session1 attempts to write value_d within its open transaction — expects `WT_ROLLBACK` (conflicting update). Verifies data after rollback shows value_b (not value_d).
- **Components:** `txn.c`, `txn_timestamp.c`, `evict.c`, `history_store.c`
- **Notes:** Parameterized over column and integer-row formats. Regression test for a bug (fixed August 2021) where a conflict could be missed after eviction + aborted update, causing corruption (especially visible with modifies).

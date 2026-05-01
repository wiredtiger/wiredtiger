# test_txn23 — Read timestamp not cleared under cache pressure

**File:** `test/suite/test_txn23.py`
**Storage mode:** General
**Components under test:** read timestamp stability under cache pressure, history store, timestamped visibility

## Test Cases

### `test_txn23.test_txn`
- **What it tests:** Creates two tables; writes 2,000 rows at ts=20 (value_d), ts=30 (value_c), ts=40 (value_b), ts=50 (value_a) in each table with a 5MB cache; verifies all historical timestamps are visible: ts=20 → value_d, ts=30 → value_c, ts=40 → value_b, ts=50 → value_a. The small cache (5MB) and large data volume forces eviction under a running transaction with a read timestamp, testing that the read timestamp is not cleared by cache pressure.
- **Components:** `txn.c`, `txn_timestamp.c`, `history_store.c`, `evict.c`
- **Notes:** Parameterized over integer-row and column formats. Tests a regression where eviction under cache pressure could incorrectly clear a transaction's read timestamp.

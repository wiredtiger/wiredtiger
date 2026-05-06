# test_timestamp20 — History store fixup for non-timestamped updates

**File:** `test/suite/test_timestamp20.py`
**Storage mode:** General
**Components under test:** history store non-timestamped update fixup, eviction, modify operations

## Test Cases

### `test_timestamp20.test_timestamp20_standard`
- **What it tests:** Inserts value1/value2/value3 at timestamps 10/20/30 for 9999 keys; starts an old reader at ts=20; appends two no-timestamp updates (value4 without ts, value5 at ts=40); checkpoints and force-evicts pages; reads at ts=30 and confirms value4 (no-timestamp covers prior history); confirms old reader at ts=20 still sees value3 (its snapshot predates the no-timestamp write).
- **Components:** `history_store.c`, `evict.c`, `txn.c`, `txn_timestamp.c`
- **Notes:** Parameterized over string-row and column formats.

### `test_timestamp20.test_timestamp20_modify`
- **What it tests:** Applies a base value and two modifies (at ts 10, 20, 30); opens an old reader at ts=20; applies a third modify at ts=40; then two no-timestamp writes; checkpoints and force-evicts; reads at ts=30 see the no-timestamp write (value4 covers prior history); old reader at ts=20 still sees the correctly assembled modify chain (value1 + 'B' + 'C').
- **Components:** `history_store.c`, `evict.c`, `modify.c`, `txn.c`
- **Notes:** Uses `wiredtiger.Modify` to test that history-store fixup works correctly for modify chains.

# test_truncate14 — Very large namespace gaps from truncate: instantiation and checkpoint

**File:** `test/suite/test_truncate14.py`
**Storage mode:** General
**Components under test:** fast delete, large key-space gaps, page instantiation, internal page reconciliation

## Test Cases

### `test_truncate14.test_truncate`
- **What it tests:** Writes a dense blob of 1,000 rows, then 20,000 sparse rows with 1-billion-key gaps between them, then another dense blob of 1,000 rows — all at ts=20; reopens to flush; fast-truncates the entire sparse range (from first sparse key to first key of second blob) at ts=30; then takes one of three actions: (1) reads the data back before the truncation to force page instantiation, (2) checkpoints with the truncation not globally visible, or (3) makes the truncation globally visible then checkpoints. In all cases verifies the remaining 1,999 blob rows can be read correctly.
- **Components:** `btree.c`, `txn_timestamp.c`, `evict.c`, `checkpoint.c`
- **Notes:** Parameterized over column/row × instantiate/checkpoint/checkpoint-visible. Skipped on disagg if fast truncate not built. Specifically targets correctness when the namespace has billion-key sparse gaps that result in extremely large key spans in internal pages.

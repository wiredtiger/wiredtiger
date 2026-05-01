# test_rollback_to_stable25 — Comprehensive VLCS RLE cell scenarios for RTS

**File:** `test/suite/test_rollback_to_stable25.py`
**Storage mode:** General (on-disk)
**Components under test:** rollback_to_stable, column store, RLE cells, eviction

## Test Cases

### `test_rollback_to_stable25.test_rollback_to_stable25`
- **What it tests:** Systematically tests all meaningful combinations of RLE cell scenarios for column-store RTS. Writes at timestamps 10/20/30 with various key coverage patterns (uniform=all 5 keys, heterogeneous=different vals, first/middle/last key only) and operation types (none/update/delete). Evicts after timestamp 10, 20, or 30. Rolls back to ts=15 or ts=25. Uses sentinel keys (1 and 7) written at ts=5 and evicted to isolate the 5-key range (keys 2-6). Verifies expected state at ts=10/20/30 after RTS.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/col/`, `src/btree/`
- **Notes:** Column store only (`key_format=r`). Uses `filter_scenarios` to eliminate ~80% of combinations that are meaningless (e.g. cannot delete non-existent key, heterogeneous+nil is redundant, etc.). RTS verifier as teardown. `in_memory=false`. Expected state is tracked in a dict and adjusted based on rollback_time.

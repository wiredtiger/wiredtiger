# test_rollback_to_stable24 — RTS recno-counting bug fix in column store RLE cells

**File:** `test/suite/test_rollback_to_stable24.py`
**Storage mode:** General (on-disk)
**Components under test:** rollback_to_stable, column store, RLE cells, recno counting

## Test Cases

### `test_rollback_to_stable24.test_rollback_to_stable24`
- **What it tests:** Regression test for a recno-counting bug in column-store RTS. Creates an RLE cell of 3 copies of value_a at ts=10 (keys 1-3), plus value_b at key 4 at ts=10 and value_c at key 4 at ts=50. Evicts page to produce the RLE cell. Then updates key 1 to value_d at ts=30. Rolls back to ts=40. Reads at ts=40: expects key1=value_d, key2/3=value_a, key4=value_b. Prior to the fix, the bad recno count would cause RTS to use the wrong HS key and improperly tombstone key 2. Also runs on row store (`key_format=i`) to validate test correctness.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/col/`, `src/btree/col_put.c`
- **Notes:** Parametrized on key_format (column/row_integer) and worker threads (0/4/8). No prepare, no crash restart. Uses RTS verifier as teardown. `in_memory=false`.

# test_layered48 — No overflow keys or values are generated in disaggregated storage

**File:** `test/suite/test_layered48.py`
**Storage mode:** Disagg/Layered
**Components under test:** block_disagg, reconciliation (overflow handling), checkpoint, stable btree / ingest btree

## Test Cases

### `test_layered48.test_layered48`
- **What it tests:** Verifies that large keys (1000-character random strings) and large values (1000-character random strings) do not cause overflow records to be generated in disaggregated storage. Inserts 500 records with 1000-character keys and short values, checkpoints, then creates 9 updates with long values. After each checkpoint, asserts `rec_overflow_key_leaf == 0` and `rec_overflow_value == 0`.
- **Components:** block_disagg (overflow suppression / large key-value handling), reconciliation, checkpoint
- **Notes:** Parametrized over two URI prefixes (`layered:` and `table:` with `block_manager=disagg,log=(enabled=false)`). Table configured with `leaf_key_max=256,leaf_value_max=256` to define the threshold. Uses `precise_checkpoint=true`. 500 rows initial, 9 updates at keys `n*100` for n=1..9. Disagg-only.

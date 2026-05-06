# test_layered16 — Layered table cursor modify (in-place value update)

**File:** `test/suite/test_layered16.py`
**Storage mode:** Disagg/Layered (disagg_only)
**Components under test:** cursor modify operation, layered ingest btree, cur_layered.c

## Test Cases

### `test_layered16.test_modify`
- **What it tests:** Inserts a large string value (26-character alphabet repeated 5 times, 130 bytes) at key "1". Applies a `wiredtiger.Modify` that appends character 'A' at offset 130 with size 0 (pure append). Verifies via `cursor.get_value()` that the modify takes effect in the same transaction. Commits, then reads back and confirms the value is `value1 + 'A'`. Applies a second modify appending 'B' at offset 131. Confirms again via get_value during the transaction and then via a direct cursor lookup.
- **Components:** `cursor.modify()` on layered ingest btree (`cur_layered.c`), in-transaction and post-commit modify reads
- **Notes:** Parametrized by disagg_storage scenario. Tests that `wiredtiger.Modify` (partial-value update) works correctly on a layered table without a checkpoint — purely in the ingest btree. Would break if the modify path is not implemented in `cur_layered.c` or if modify results are not readable within the same transaction or after commit.

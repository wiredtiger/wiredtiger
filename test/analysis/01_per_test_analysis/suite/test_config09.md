# test_config09 — Hash bucket configuration and dirty table checkpoint stats

**File:** `test/suite/test_config09.py`
**Storage mode:** General
**Components under test:** connection config (hash buckets), checkpoint stats

## Test Cases

### `test_config09.test_config09_invalid`
- **What it tests:** Hash bucket count that is not a power of 2; expects error.
- **Components:** `src/conn/conn_open.c`, `src/config/`
- **Notes:** Skipped for tiered hook.

### `test_config09.test_config09`
- **What it tests:** Valid hash bucket configs (powers of 2); verifies dirty table checkpoint statistics are tracked correctly.
- **Components:** `src/conn/conn_open.c`, `src/btree/`, `src/checkpoint/`
- **Notes:** Skipped for tiered hook. Checks `checkpoint_pages_dirty` or similar checkpoint stat after modifications.

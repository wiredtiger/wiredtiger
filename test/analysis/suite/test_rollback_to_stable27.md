# test_rollback_to_stable27 — RTS handles mix of timestamped and non-timestamped updates on VLCS RLE cell

**File:** `test/suite/test_rollback_to_stable27.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, column store, RLE cells, non-timestamp updates

## Test Cases

### `test_rollback_to_stable27.test_rollback_to_stable`
- **What it tests:** Verifies RTS correctly handles a scenario where a non-timestamped update (`no_timestamp=true`) is mixed with timestamped updates on a VLCS RLE cell. Writes value_a to 10 keys at ts=20, evicts the page (producing an RLE cell), then writes value_b to key 7 without a timestamp. Rolls back to stable=15 (below ts=20). After RTS: only key 7 with value_b should be visible at any timestamp (value_a was completely rolled back, value_b is timestamp-less and survives).
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/col/`, `src/btree/`
- **Notes:** Parametrized on key_format (column/row_integer) and in_memory (true/false), worker threads (0/4/8). Row store included as control group. No checkpoint or crash-restart. `cache_size` not specified (uses default). In-memory mode disables logging.

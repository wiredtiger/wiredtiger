# test_timestamp05 — Timestamps do not end up in metadata

**File:** `test/suite/test_timestamp05.py`
**Storage mode:** General
**Components under test:** schema creation with timestamps, bulk load with timestamps, checkpoint with `use_timestamp=true`

## Test Cases

### `test_timestamp05.test_create`
- **What it tests:** Sets oldest=50/stable=50, creates a table inside a transaction committed at timestamp=100, inserts a non-timestamped dirty record, then checkpoints at stable=50. Verifies this does not crash or corrupt.
- **Components:** `schema.c`, `txn_timestamp.c`, `checkpoint.c`
- **Notes:** Parameterized over integer-row and column formats.

### `test_timestamp05.test_bulk`
- **What it tests:** Creates a table, opens a bulk cursor and inserts 100 keys; sets oldest=50/stable=50; commits the bulk cursor close inside a transaction at timestamp=100; inserts one more non-timestamped record; checkpoints at stable=50.
- **Components:** `cursor_bulk.c`, `txn_timestamp.c`, `checkpoint.c`
- **Notes:** Skipped for disagg (bulk load not supported). Parameterized over integer-row and column formats. Tests that bulk-loaded data committed at a timestamp beyond stable does not appear after checkpoint at stable.

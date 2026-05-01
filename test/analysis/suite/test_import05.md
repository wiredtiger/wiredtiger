# test_import05 — Import errors when file timestamps exceed the global timestamp

**File:** `test/suite/test_import05.py`
**Storage mode:** General
**Components under test:** schema/import, timestamp validation, txn timestamps

## Test Cases

### `test_import05.test_file_import_ts_past_global_ts`
- **What it tests:** Verifies that importing a file fails when its aggregated timestamps (newest start durable / newest stop durable) are newer than the configured global timestamp (oldest or stable), and succeeds once the global timestamp is advanced to match.
- **Components:** `src/schema/schema_create.c`, `src/txn/txn_timestamp.c`, `src/block/block_ckpt.c`
- **Notes:** Parameterized by three axes (8 scenarios total):
  - `op_type`: `insert` or `delete` — determines which aggregated timestamp is checked (`newest start durable` vs `newest stop durable`).
  - `repair`: `True` or `False` — whether to use `repair=true` or `file_metadata=(...)`.
  - `global_ts`: `oldest` or `stable` — which global timestamp to compare against (via `compare_timestamp=stable_timestamp` option).

  Test steps:
  1. Insert N-1 records and checkpoint.
  2. Perform final insert or delete at the highest timestamp and checkpoint.
  3. Open new DB, try import with default global ts=0 → expect error on "newest start durable".
  4. Set global ts to (last_ts - 1) → still expect error (either start or stop depending on op_type).
  5. Set global ts = last_ts → expect success.

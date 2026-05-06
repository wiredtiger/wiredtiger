# test_assert06 — Timestamp ordering enforcement via write_timestamp_usage=ordered

**File:** `test/suite/test_assert06.py`
**Storage mode:** General
**Components under test:** timestamps, write_timestamp_usage assertion, transaction API

## Test Cases

### `test_assert06.test_timestamp_alter`
- **What it tests:** Starts with a table that has no timestamp enforcement, writes data with and without timestamps (mixed usage), then alters the table to `write_timestamp_usage=ordered`. After the alter, confirms that a commit without a timestamp (after a timestamped write to the same key) raises the expected `use timestamps once they are first used` error. Skipped on diagnostic builds.
- **Components:** `src/txn/txn_timestamp.c`, `src/schema/schema_alter.c`, `src/btree/bt_curprev.c`
- **Notes:** Uses `oldest_timestamp` advancement to allow the alter to close the file. Parametrized across `row` (S key) and `var` (r key) formats.

### `test_assert06.test_timestamp_usage`
- **What it tests:** Creates a table with `write_timestamp_usage=ordered,assert=(write_timestamp=on)` upfront. Tests the following scenarios against the assertion: (1) successful ordered inserts; (2) inserting at a lower timestamp than a sibling key in the same transaction raises `/unexpected timestamp usage/`; (3) committing without a timestamp after a timestamped write raises the usage error; (4) setting timestamp at beginning, middle, or end of transaction; (5) `prepare_transaction` + `durable_timestamp` on commit; (6) rollback after prepare does not fire an assertion.
- **Components:** `src/txn/txn_timestamp.c`, `src/txn/txn.c`
- **Notes:** Parametrized across `row` (S) and `var` (r) key formats. Skipped on diagnostic builds.

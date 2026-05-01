# test_truncate01 — Truncate arguments, URI truncate, cursor-range truncation, and timestamp handling

**File:** `test/suite/test_truncate01.py`
**Storage mode:** General
**Components under test:** `truncate` API, cursor-range truncate, timestamp visibility, fast delete path

## Test Cases

### `test_truncate_arguments.test_truncate`
- **What it tests:** Validates that incorrect truncate argument combinations (e.g., both URIs and cursors, neither URI nor cursor) raise `WT_INVALID_ARG`. Also confirms that truncating a non-existent URI raises the correct error.
- **Components:** `session.c`, `schema.c`
- **Notes:** Covers argument validation logic in `__wt_schema_truncate`.

### `test_truncate_uri.test_truncate`
- **What it tests:** Inserts a full dataset, truncates by URI (entire table), and verifies the table is empty afterward. Parameterized over string-row, integer-row, and column formats.
- **Components:** `schema.c`, `btree.c`
- **Notes:** Uses `SimpleDataSet` from `wtdataset`; exercises the full-table URI truncate path.

### `test_truncate_cursor_order.test_truncate`
- **What it tests:** Truncates using start/stop cursor pairs in all combinations (start only, stop only, start+stop, both None) to verify correct range deletion. Verifies that only records outside the range survive.
- **Components:** `btree.c`, `cursor.c`, `schema.c`
- **Notes:** Parameterized over string-row, integer-row, and column key formats. Exercises multiple cursor-position configurations.

### `test_truncate_cursor_end.test_truncate`
- **What it tests:** Positions the cursor at the last key and truncates from that position to end; verifies only that last record is deleted. Tests start=last, stop=None edge case.
- **Components:** `btree.c`, `cursor.c`
- **Notes:** End-of-object truncation edge case.

### `test_truncate_empty.test_truncate`
- **What it tests:** Runs cursor-range truncate on an empty table; verifies no errors occur and the table remains empty.
- **Components:** `btree.c`, `cursor.c`
- **Notes:** Regression guard for truncate on empty collections.

### `test_truncate_timestamp.test_truncate`
- **What it tests:** Inserts records at timestamp 1, truncates at timestamp 2, checkpoints; verifies that reading at ts=1 shows original records, reading at ts=2 shows them deleted.
- **Components:** `txn_timestamp.c`, `btree.c`, `schema.c`, `checkpoint.c`
- **Notes:** Tests timestamped truncate visibility. Parameterized over formats.

### `test_truncate_cursor.test_truncate`
- **What it tests:** Inserts a dataset, truncates a range, verifies expected keys remain and truncated keys are absent. Checks that a second truncate on the same range succeeds without error.
- **Components:** `btree.c`, `cursor.c`, `schema.c`
- **Notes:** Parameterized over multiple key formats and dataset configurations (row, column, simple, complex).

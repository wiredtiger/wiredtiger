# test_verify — wt verify CLI and API: corruption detection, redaction, and abort-on-first-error

**File:** `test/suite/test_verify.py`
**Storage mode:** General (some tests skipped for disagg — cannot access shared table data directly)
**Components under test:** `wt verify`, `session.verify`, checksum error detection, `read_corrupt`, `dump_address`, `dump_pages`, redacted output, verify-all

## Test Cases

### `test_verify.test_verify_process_empty`
- **What it tests:** Runs `wt verify table:<name>` on an empty table; verifies success (no error).
- **Components:** `util_verify.c`, `verify.c`
- **Notes:** No parameterization. Basic smoke test.

### `test_verify.test_verify_process`
- **What it tests:** Populates a table with 1,000 entries (cumulative string keys/values); runs `wt verify table:<name>`; verifies success.
- **Components:** `util_verify.c`, `verify.c`
- **Notes:** No parameterization.

### `test_verify.test_verify_api_empty`
- **What it tests:** Calls `session.verify('table:<name>', None)` on an empty table; verifies success.
- **Components:** `verify.c`
- **Notes:** No parameterization.

### `test_verify.test_verify_api`
- **What it tests:** Populates a table with 1,000 entries; calls `session.verify`; then reads back all records and verifies they match the original data.
- **Components:** `verify.c`
- **Notes:** No parameterization. Data integrity check combined with verify.

### `test_verify.test_verify_api_75pct_null`
- **What it tests:** Populates a table; writes 4,096 null bytes at 75% file offset; reopens connection; calls `session.verify(..., "read_corrupt")` and asserts `WiredTigerError`; runs `wt -p verify -d dump_address ... -d` and verifies exactly 1 "Read failure" line in dump output and at least 1 "read checksum error" in stderr.
- **Components:** `verify.c`, `block.c`
- **Notes:** Skipped for disagg (FIXME-WT-15064). Tests `read_corrupt` option which continues past checksum errors.

### `test_verify.test_verify_api_read_corrupt_pages`
- **What it tests:** Corrupts 3 locations (25%, 50%, 75%) with invalid bytes; runs `wt -p verify -d dump_address`; verifies exactly 1 "Read failure" message in dump output.
- **Components:** `verify.c`, `block.c`
- **Notes:** Skipped for disagg. Tests multiple corruption points; `dump_address` stops at first failure.

### `test_verify.test_verify_api_corrupt_first_page`
- **What it tests:** Populates a table; runs `wt verify -d dump_address` to find the first leaf page offset; corrupts bytes at the midpoint of that page range; reopens; runs `wt -p verify -d dump_address` (expects failure) and `session.verify(..., "read_corrupt")` (expects `WiredTigerError`); verifies exactly 1 checksum error in stderr and 1 "Read failure" in dump output.
- **Components:** `verify.c`, `block.c`
- **Notes:** Skipped for disagg. Tests that corruption of the first child of an internal node is detected.

### `test_verify.test_verify_process_75pct_null`
- **What it tests:** Corrupts 75% with 4,096 null bytes; runs `wt -p verify -d dump_address` (expects failure, 1 "Read failure" in dump); runs `wt -p verify -c` (expects failure with "read checksum error" in stderr).
- **Components:** `util_verify.c`, `verify.c`, `block.c`
- **Notes:** Skipped for disagg. Tests `wt verify` CLI with and without `-c` (continue-on-error) flag.

### `test_verify.test_verify_process_25pct_junk`
- **What it tests:** Corrupts 25% with `\x01\xff\x80` garbage bytes; runs `wt -p verify -d dump_address` (expects failure, 1 "Read failure"); runs `wt -p verify -c` (expects failure with "read checksum error").
- **Components:** `util_verify.c`, `verify.c`, `block.c`
- **Notes:** Skipped for disagg. Same as 75pct_null but at the 25% position.

### `test_verify.test_verify_process_read_corrupt_pages`
- **What it tests:** Corrupts 3 locations (25%, 75%, 80%); runs `wt -p verify -c` (expects failure); runs `wt -p verify -d dump_address` (expects failure, 1 "Read failure"); asserts at least 1 "read checksum error" in stderr (may not detect all 3 if some are free space or child pages under a corrupted parent).
- **Components:** `util_verify.c`, `verify.c`, `block.c`
- **Notes:** Skipped for disagg. Tests multiple-corruption detection with continue-on-error mode.

### `test_verify.test_verify_process_truncated`
- **What it tests:** Truncates the file at 75% offset; runs `wt -p verify`; expects failure.
- **Components:** `util_verify.c`, `verify.c`, `block.c`
- **Notes:** Skipped for disagg. Truncated file = incomplete/missing blocks.

### `test_verify.test_verify_process_zero_length`
- **What it tests:** Truncates the file to zero length; runs `wt verify`; expects failure.
- **Components:** `util_verify.c`, `verify.c`, `block.c`
- **Notes:** Skipped for disagg.

### `test_verify.test_verify_redacted`
- **What it tests:** Skipped if not a diagnostic build (`diagnostic_build()` returns false); inserts a `secret_key` = `#hidden#` record; checkpoints; runs `wt -p verify -d dump_pages file:<name>.wt` without `-u` and verifies the output does NOT contain `secret_key` or `#hidden#`; runs again with `-u` (unredacted) and verifies both strings ARE present.
- **Components:** `util_verify.c`, `verify.c`
- **Notes:** Requires diagnostic build. Tests that `dump_pages` mode redacts user data by default and reveals it with `-u`.

### `test_verify.test_verify_all`
- **What it tests:** Creates 3 tables, populates them, checkpoints; runs `wt verify` (no URI, verifies all); then corrupts tables 1 and 2 (at 75% with null bytes); runs `wt -p verify -a` (abort-on-first-error); expects failure; verifies exactly 1 error for table 1 (a1) and 0 errors for table 2 (a2) — i.e., verify aborted after the first corrupted table.
- **Components:** `util_verify.c`, `verify.c`
- **Notes:** Skipped for disagg. Tests the `-a` (abort) flag that stops at the first failing table.

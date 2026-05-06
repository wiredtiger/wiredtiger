# test_timestamp16 — Read timestamp cleared after transaction end

**File:** `test/suite/test_timestamp16.py`
**Storage mode:** General
**Components under test:** read timestamp lifecycle, `last_checkpoint` timestamp, checkpoint `use_timestamp`

## Test Cases

### `test_timestamp16.test_read_timestamp_cleared`
- **What it tests:** Opens a transaction with `read_timestamp=100`, rolls it back; checkpoints with `use_timestamp=true`; confirms `get=last_checkpoint` returns 0 (read_timestamp should not affect checkpoint). Sets stable=2; again starts then rolls back a transaction with read_timestamp=100; checkpoints; confirms `last_checkpoint` == 2. Repeats with commit (not rollback) to confirm commit also clears the read timestamp.
- **Components:** `txn_timestamp.c`, `checkpoint.c`
- **Notes:** Verifies that a read timestamp from a completed transaction does not hold the checkpoint at an incorrect point.

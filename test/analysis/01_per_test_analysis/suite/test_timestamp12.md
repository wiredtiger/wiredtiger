# test_timestamp12 — use_timestamp close configuration effect on recovery

**File:** `test/suite/test_timestamp12.py`
**Storage mode:** General
**Components under test:** `close_conn` with `use_timestamp`, checkpoint durability, recovery

## Test Cases

### `test_timestamp12.test_timestamp_recovery`
- **What it tests:** Creates a logged table and a non-logged (checkpoint-durable) table; inserts 9 entries in the stable range and 9 more entries beyond stable; closes with `use_timestamp=true/false/default`; reopens; verifies logged table always sees all 18 entries (committed via log), while the non-logged table sees either 9 or 18 entries depending on `use_timestamp` (true/default means only stable-range data; false means all data).
- **Components:** `txn_timestamp.c`, `checkpoint.c`, `recovery.c`, `log.c`
- **Notes:** Parameterized over integer-row/column formats × 3 close configs. Tests that `use_timestamp=false` at close checkpoints all dirty data regardless of stable timestamp.

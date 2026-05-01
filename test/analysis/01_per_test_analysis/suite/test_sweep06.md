# test_sweep06 — Table dhandles not swept while file data dhandles present

**File:** `test/suite/test_sweep06.py`
**Storage mode:** General
**Components under test:** file manager sweep, dhandle expiry, cursor caching, multi-threaded access

## Test Cases

### `test_sweep06.test_dhandles`
- **What it tests:** Creates 199 tables; uses 100 threads per iteration for 99 iterations, each thread opening a cursor and inserting 100 records in its own session; after all threads complete, reads `dh_sweep_dead_close` and `dh_sweep_expired_close` and asserts both are 0 — confirming that table dhandles are not swept while data file dhandles remain referenced.
- **Components:** `file_manager.c`, `dhandle.c`, `cursor.c`
- **Notes:** Skipped for disagg (multi-threaded incompatibility). Parameterized over cursor caching disabled/enabled. Configuration: `close_idle_time=60`, `close_scan_interval=30` (long idle time to prevent normal sweeping during the test). Verbose sweep at level 3.

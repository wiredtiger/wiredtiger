# test_cursor08 — Log cursor with compressed log files

**File:** `test/suite/test_cursor08.py`
**Storage mode:** General
**Components under test:** log cursor, log compression (snappy, zlib, none), WAL reading

## Test Cases

### `test_cursor08.test_log_cursor`
- **What it tests:** Inserts data into a logged table, compresses the log with the configured compressor, then reads all log records via a log cursor. Verifies that all expected entries survive compression and decompression correctly.
- **Components:** `src/log/`, `src/cursor/cur_log.c`, `src/compressor/`
- **Notes:** Scenarios: `regular`/`reopen` × `nop`/`snappy`/`zlib`/`none` compression. Tests that log cursor can decode compressed log files.

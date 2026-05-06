# test_corrupt01 — Block corruption detection via checksum error

**File:** `test/suite/test_corrupt01.py`
**Storage mode:** General
**Components under test:** block manager, checksum validation, verbose logging

## Test Cases

### `test_corrupt01.test_corrupt01`
- **What it tests:** Creates a table, checkpoints it, removes some keys, dumps block addresses via verbose output, corrupts a raw leaf-page block on disk, then reopens and reads the table to trigger a checksum error. Verifies that verbose block dump and extent list messages appear in output.
- **Components:** `src/block/`, `src/btree/`, `src/os_posix/`
- **Notes:** Skipped for disagg hook. `conn_config = 'cache_size=100MB,statistics=(all),debug_mode=(corruption_abort=false)'`. Uses `corruption_abort=false` to avoid process exit on corruption. Verifies `WT_ERROR` from read after corruption. Checks for verbose block manager output.

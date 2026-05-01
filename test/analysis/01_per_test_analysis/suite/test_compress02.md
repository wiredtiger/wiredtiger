# test_compress02 — zstd compression level reconfiguration after restart

**File:** `test/suite/test_compress02.py`
**Storage mode:** General
**Components under test:** zstd block compressor, compression level reconfiguration, crash recovery

## Test Cases

### `test_compress02.test_compress02`
- **What it tests:** Verifies that the zstd compression level can be reconfigured to a different value after a crash restart without corrupting or losing data. Tables created with the original compression level (6) remain readable after reopening with a new level (9).
- **Components:** `src/compressor/zstd_compress.c`, `src/conn/conn_open.c`
- **Notes:** Decorated with `@wttest.zstdtest('Skip zstd on pcc and zseries machines')`. Extension loaded via `conn_extensions` with `skip_if_missing=True`. Creates table with `block_compressor=zstd` at level 6, writes 1 000 rows, checkpoints, simulates crash via `copy_wiredtiger_home(., RESTART)`, reopens with `compression_level=9`. Verifies all 1 000 rows still return the expected value. Tests that the compression level is per-session/connection config, not stored per-table, so existing compressed blocks remain decompressable with any level.

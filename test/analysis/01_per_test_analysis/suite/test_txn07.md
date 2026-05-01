# test_txn07 — Commits and rollbacks for truncate operations with log compression

**File:** `test/suite/test_txn07.py`
**Storage mode:** General
**Components under test:** truncate, log compression (nop/snappy/zlib/none), `log_compress_*` stats

## Test Cases

### `test_txn07.test_ops`
- **What it tests:** Sets up a table with 5 entries of large compressible string values (3MB each); performs a truncate (all, both, start, or stop); verifies isolation at all levels and after backup/recovery; commits or rolls back; after all operations checks log compression statistics: for no compression (`''`) compressed length equals uncompressed length and write count is 0; for `nop` compressor compressed == uncompressed and either fails or is small; for real compressors compressed < uncompressed and write count > 0. Runs `wt printlog` on the backup.
- **Components:** `txn.c`, `log.c`, `compress.c`, `cursor.c`, `backup.c`
- **Notes:** Parameterized over row/var × truncate-all/both/start/stop × commit/rollback × nop/snappy/zlib/none (pruned to 30 default / 1000 long). Cycles through 4 sync modes. Tests that log records containing truncate operations are correctly compressed and that `log_compress_*` stats reflect the outcome.

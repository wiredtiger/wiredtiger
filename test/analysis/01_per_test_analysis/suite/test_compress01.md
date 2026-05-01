# test_compress01 — Smoke test for all supported block compressors

**File:** `test/suite/test_compress01.py`
**Storage mode:** General
**Components under test:** block compression (nop, lz4, snappy, zlib, zstd, iaa), file and table URIs

## Test Cases

### `test_compress01.test_compress`
- **What it tests:** Smoke test verifying that all supported compressors (nop, lz4, snappy, zlib, zstd, iaa) correctly compress and decompress data across file and table URI types. Writes 10 000 records mixing overflow values (bigvalue = 10 KB) and small values, forces pages to disk via reopen, then reads all records and verifies correctness.
- **Components:** `src/compressor/`, `src/block/block_read.c`, `src/block/block_write.c`
- **Notes:** 16 scenarios from cross-product of types (file, table) × compress (8 compressors). Extension loaded via `conn_extensions` with `skip_if_missing=True` (test is skipped if compressor is not built). `leaf_page_max=4096` forces overflow items for every 12th record. lz4-noraw and zlib-noraw variants test that the old raw-mode API names still function (API compatibility). Both small and overflow paths are exercised.

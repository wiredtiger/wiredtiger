# test_import06 — Import a file using the repair option (no metadata needed)

**File:** `test/suite/test_import06.py`
**Storage mode:** General
**Components under test:** schema/import (repair mode), block manager, compressors, encryptors, metadata reconstruction

## Test Cases

### `test_import06.test_import_repair`
- **What it tests:** Imports a file using `import=(enabled,repair=true)` without providing `file_metadata`, relying on WiredTiger to reconstruct the metadata from the file's on-disk content. Verifies data correctness and that reconstructed metadata matches the original.
- **Components:** `src/schema/schema_create.c`, `src/block/`, `src/compress/`, `src/encrypt/`
- **Notes:** Parameterized by three axes (96 scenarios):
  - `allocsize`: 512, 1024, 2048, 4096 bytes
  - `compressor`: none, nop, lz4, snappy, zlib, zstd (skipped if extension missing)
  - `encryptor`: none, nop, rotn, sodium (sodium uses 256-bit hex secretkey for chacha20)

  Both connection-level and table-level encryption/compression config are set. After repair import, the test compares `metadata:` output (excluding id and checkpoint fields) against the original to verify reconstruction accuracy.

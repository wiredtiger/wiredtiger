# test_import09 — Import a table using the repair option (no exported metadata)

**File:** `test/suite/test_import09.py`
**Storage mode:** General (skipped for tiered storage)
**Components under test:** schema/import (repair mode), table layer, block manager, compressors, encryptors

## Test Cases

### `test_import09.test_import_table_repair`
- **What it tests:** Imports a full `table:` object using `import=(enabled,repair=true)` without any provided metadata. Reconstructs both file-level and table-level metadata from the on-disk `.wt` file. Verifies data correctness and compares reconstructed metadata against the original.
- **Components:** `src/schema/schema_create.c`, `src/schema/schema_table.c`, `src/block/`, `src/compress/`, `src/encrypt/`
- **Notes:** Parameterized by four axes (192 scenarios):
  - `tables`: `simple_table` (`key_format=r,value_format=i`, 100 rows) or `table_with_named_columns` (`key_format=r,value_format=SSi`, 6 country rows)
  - `allocsize`: 512, 1024, 2048, 4096 bytes
  - `compressor`: none, nop, lz4, snappy, zlib, zstd
  - `encryptor`: none, nop, rotn, sodium (sodium with hex secretkey)

  Compares both `file:` and `table:` metadata entries (excluding id/checkpoint) after repair import. Decorated with `@wttest.skip_for_hook("tiered")`.

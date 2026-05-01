# test_encrypt01 — Basic block encryption round-trip

**File:** `test/suite/test_encrypt01.py`
**Storage mode:** General
**Components under test:** encryptors (nop, rotn, sodium), compressors (nop, lz4, snappy, zlib, zstd), block manager, btree

## Test Cases

### `test_encrypt01.test_encrypt`
- **What it tests:** Creates a table with 5,000 records containing random-length keys and values (up to 10 KB), closes the connection to flush to disk, reopens (reading encrypted/compressed pages from disk), and verifies every record can be found with the correct value. Exercises combinations of system-level and per-table encryption with several compressors.
- **Components:** `src/block/`, `src/btree/`, `ext/encryptors/`, `ext/compressors/`
- **Notes:** Scenarios: URI types `file:` vs `table:`; system encryption: none, nop, rotn (keyid=11 system, keyid=13 table), rotn-none, sodium; compression: none, nop, lz4, snappy, zlib, zstd, none-snappy, snappy-lz4; early extension load: true/false. Extensions are skipped if not present (`skip_if_missing=True`). Sodium encryptor uses a fixed 256-bit hex secretkey for testing.

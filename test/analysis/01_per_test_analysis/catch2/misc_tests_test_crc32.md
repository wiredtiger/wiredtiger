# test_crc32 — CRC32c checksum function tests

**File:** `test/catch2/misc_tests/test_crc32.cpp`
**Storage mode:** General
**Components under test:** `wiredtiger_crc32c_func`, `wiredtiger_crc32c_with_seed_func`
**Test type:** Unit

## TEST_CASE: "CRC32c checksums" [crc32]
- **What it tests:**
  - A zero-length input returns the expected CRC32c value (seed-only result).
  - Known input data produces a known expected CRC32c value (regression / golden value check).
  - Computing the checksum in two chunks (chunked computation) produces the same result as computing it over the whole buffer at once.
  - `wiredtiger_crc32c_with_seed_func` chains correctly: the output of the first chunk is used as the seed for the second chunk.
- **Components:** `wiredtiger_crc32c_func`, `wiredtiger_crc32c_with_seed_func`
- **Notes:** Tests both the hardware-accelerated and software fallback paths (the function pointer selects the appropriate implementation at runtime).

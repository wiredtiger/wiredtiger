# wt4117_checksum — CRC32C public API smoke test

**Path:** `test/csuite/wt4117_checksum/`
**Language:** C
**Storage mode:** General
**Jira ticket:** WT-4117
**Components under test:** `wiredtiger_crc32c_func()` public API, CRC32C correctness for null bytes and known strings

## What This Test Does
This test is a smoke test for the `wiredtiger_crc32c_func()` public API. It retrieves the hardware-or-software CRC32C function pointer and verifies it against six known-good checksums: four null-byte inputs of lengths 1–4, and two well-known ASCII strings ("123456789" and "The quick brown fox jumps over the lazy dog"). The test does not open a WiredTiger connection — it exercises only the standalone checksum function.

## Test Scenarios / Cases

### Scenario: Known-value CRC32C checks via public API
- **What it tests:** That the CRC32C function returned by `wiredtiger_crc32c_func()` produces correct results for null-byte buffers of length 1, 2, 3, and 4, and for two well-known ASCII strings.
- **Components:** `wiredtiger_crc32c_func()`, hardware/software CRC32C dispatch.
- **Notes:** Expected values: null×1 = 0x527d5351, null×2 = 0xf16177d2, null×3 = 0x6064a37a, null×4 = 0x48674bc7, "123456789" = 0xe3069283, "The quick brown fox..." = 0x22620404. No WiredTiger connection required.

## LazyFS Variant
None.

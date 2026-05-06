# wt2695_checksum — CRC32C hardware/software checksum equivalence test

**Path:** `test/csuite/wt2695_checksum/`
**Language:** C
**Storage mode:** General (WiredTiger opened for context; no data tables)
**Jira ticket:** WT-2695
**Components under test:** `__wt_checksum` (hardware CRC32C), `__wt_checksum_sw` (software CRC32C), `wiredtiger_crc32c_with_seed_func`, `__wt_checksum_with_seed_sw`

## What This Test Does
This test smoke-tests the CRC32C checksum implementation by: (1) verifying known-good checksums for specific byte sequences (null bytes and 0xFF bytes of various lengths, and two ASCII strings); (2) confirming that the hardware and software implementations agree on randomly generated data of both power-of-two and random sizes; and (3) verifying the cumulative (seeded) checksum API produces the same result as a single-shot checksum regardless of chunk size. Misaligned reads are tested explicitly.

## Test Scenarios / Cases

### Scenario: Known-value validation
- **What it tests:** That the hardware and software CRC32C implementations produce specific hard-coded values for null bytes (1–4 bytes), 0xFF bytes (1–9 bytes), and two well-known ASCII strings.
- **Components:** `__wt_checksum`, `__wt_checksum_sw`.
- **Notes:** Values can be cross-checked against reference CRC32C implementations.

### Scenario: Cumulative seeded checksum
- **What it tests:** That computing a CRC32C cumulatively over chunks of various sizes (using `wiredtiger_crc32c_with_seed_func` / `__wt_checksum_with_seed_sw`) produces the same result as a single-pass checksum over the entire buffer.
- **Components:** `wiredtiger_crc32c_with_seed_func`, `__wt_checksum_with_seed_sw`, cumulative calculation.
- **Notes:** s390x is explicitly excluded from software cumulative tests due to WT-12067.

### Scenario: Random power-of-two data (512B to DATASIZE, 1000 iterations)
- **What it tests:** That hardware and software checksums agree on randomly filled buffers whose lengths are powers of two, with additional cumulative checks over random chunk sizes.
- **Components:** `__wt_checksum`, `__wt_checksum_sw`, `__wt_random`.

### Scenario: Random arbitrary-length data (1000 iterations)
- **What it tests:** Same hardware/software agreement check and cumulative check for buffers with completely random sizes up to 128 KB.
- **Components:** `__wt_checksum`, `__wt_checksum_sw`.

### Scenario: Strobed misalignment (0–15 bytes offset, 0–15 bytes length)
- **What it tests:** That hardware and software agree on every combination of small size (0–15) and misalignment (0–15) using 0xFF data.
- **Components:** `__wt_checksum`, `__wt_checksum_sw`, unaligned pointer arithmetic.

## LazyFS Variant
None.

# packing — Integer and struct packing/unpacking unit tests

**Path:** `test/packing/`
**Language:** C
**Storage mode:** General (no WiredTiger database opened, except library init)
**Components under test:** `__wt_vpack_uint`, `__wt_vunpack_uint`, `__wt_vpack_int`, `__wt_vunpack_int`, `__wt_struct_packv`, `__wt_struct_sizev`, `__wt_4b_pack_array`, `__wt_4b_unpack_array`, zigzag signed encoding

## Overview

Five independent programs exercise WiredTiger's internal integer and struct packing codecs without opening a live database. They verify round-trip correctness for variable-length integer encoding (VLQ), format-string-based struct packing, and the newer 4-bit-nibble array codec. Boundary values, error conditions, and random fuzzing are all covered.

## Test Scenarios / Cases

### Scenario: Format-string struct packing (`packing-test.c`)
- **What it tests:** `__wt_struct_sizev` and `__wt_struct_packv` with valid format strings (`"iii"`, `"3i"`, `"iS"`, `"s"`, `".s"`) and asserts that invalid format specifiers (`>s`, `<s`, `@s`) return `EINVAL`.
- **Components:** `__wt_struct_packv`, `__wt_struct_sizev`, format string parser
- **Notes:** Output (hex bytes) is printed for each valid format to aid debugging. The WT_ITEM (`u` format) test is commented out as a TODO.

### Scenario: VLQ unsigned integer performance / correctness (`intpack-test.c`)
- **What it tests:** For each power-of-2 value from 1 to 2^45, encodes via `__wt_vpack_uint` and immediately decodes via `__wt_vunpack_uint`, asserting the round-trip value matches. Runs 10 million outer iterations as a micro-benchmark.
- **Components:** `__wt_vpack_uint`, `__wt_vunpack_uint`
- **Notes:** The alternative `memmove` path (for comparison) is conditionally compiled out. Confirms that encoded length never exceeds `WT_INTPACK64_MAXSIZE`.

### Scenario: VLQ signed and unsigned display (`intpack-test2.c`)
- **What it tests:** For each power-of-2 from 1 to 2^59, prints the VLQ hex encoding of both the positive value (`__wt_vpack_uint`) and its negation (`__wt_vpack_int`). Asserts encoded lengths are within bounds.
- **Components:** `__wt_vpack_uint`, `__wt_vpack_int`
- **Notes:** Output only; no decode round-trip. Serves as a visual inspection tool for the encoding scheme.

### Scenario: VLQ signed round-trip with boundary spreads (`intpack-test3.c`)
- **What it tests:** For each value in ranges centred at 0, INT16_MAX, INT32_MAX, and INT64_MAX (each with ±1025 spread), and for halving values down from INT64_MAX: encodes with `__wt_vpack_int` and `__wt_vpack_uint`, decodes back, and asserts exact value recovery and exact buffer-pointer advancement.
- **Components:** `__wt_vpack_int`, `__wt_vunpack_int`, `__wt_vpack_uint`, `__wt_vunpack_uint`
- **Notes:** Also tests that negative values are round-tripped as unsigned (the VLQ unsigned encoding of a reinterpreted bit-pattern).

### Scenario: 4-bit nibble array codec — positive integers, signed zigzag, arrays, extremes (`int4bpack-test.c`)
- **What it tests:** Full round-trip testing of the `__wt_4b_pack_array` / `__wt_4b_unpack_array` codec, including: values 0–200 and various larger positives; signed integers -100 to +100 via `__wt_encode_signed_as_positive` / `__wt_decode_positive_as_signed` (zigzag); pairs and arrays of small and large integers; UINT64_MAX; INT64_MIN, -1, 0, 1, INT64_MAX; nibble-count boundary values (7, 8, 15, 16, 63, 64, 511, 512); alignment-flip arrays; exact-fit success and one-byte-short ENOMEM; truncated/overcount decode EINVAL; partial-decode-resume via `__4b_unpack_init` / `__4b_unpack_posint_ctx`; and 4000-iteration randomised fuzz (positive and signed).
- **Components:** `__wt_4b_pack_array`, `__wt_4b_unpack_array`, `__wt_4b_size_array`, `__wt_encode_signed_as_positive`, `__wt_decode_positive_as_signed`, `__4b_unpack_init`, `__4b_unpack_posint_ctx`
- **Notes:** Most comprehensive of the five; tests all error paths (ENOMEM, EINVAL) and the stateful partial-decode API.

## Coverage Notes

The packing suite provides thorough unit-level coverage of WiredTiger's internal serialisation codecs, independent of storage or concurrency. It uniquely covers the 4-bit nibble array codec (`int4bpack-test.c`) including error paths and partial decode, which is not exercised elsewhere. The signed VLQ round-trip spread tests (`intpack-test3.c`) catch off-by-one errors at integer-type boundaries. Gaps: the `u` (WT_ITEM / raw bytes) format string is not tested in `packing-test.c`; there is no test for the combined struct pack/unpack round-trip (only pack is checked against expected hex); no multi-threaded concurrency testing of the codecs.

# test_bitstring — Bitstring macro unit tests

**File:** `test/catch2/block/unit/test_bitstring.cpp`
**Storage mode:** General
**Components under test:** Bitstring macros (`__bit_byte`, `__bit_mask`, `__bitstr_size`, `__bit_nset`)
**Test type:** Unit

## TEST_CASE: "__bit_byte" [bitstring]
- **What it tests:** Computes the correct byte index for a given bit position.
- **Components:** `__bit_byte` macro
- **Notes:** Verifies bit-to-byte index formula: `bit / 8`.

## TEST_CASE: "__bit_mask" [bitstring]
- **What it tests:** Produces the correct single-bit mask for a given bit position within its byte.
- **Components:** `__bit_mask` macro
- **Notes:** Mask is `1 << (bit % 8)`.

## TEST_CASE: "__bitstr_size" [bitstring]
- **What it tests:** Computes the number of bytes needed to hold `nbits` bits.
- **Components:** `__bitstr_size` macro
- **Notes:** Result is `(nbits + 7) / 8` (ceiling division).

## TEST_CASE: "__bit_nset" [bitstring]
### SECTION: "aligned - exact byte boundary"
- **What it tests:** Setting a range of bits that aligns exactly to byte boundaries.
- **Components:** `__bit_nset` macro
- **Notes:** All bits in the target range are set; surrounding bits are unaffected.

### SECTION: "unaligned - start in middle of byte"
- **What it tests:** Setting a range that begins partway through a byte.
- **Components:** `__bit_nset` macro
- **Notes:** Only the intended bits within the starting byte are set.

### SECTION: "unaligned - end in middle of byte"
- **What it tests:** Setting a range that ends partway through a byte.
- **Components:** `__bit_nset` macro
- **Notes:** Only the intended bits within the ending byte are set.

### SECTION: "unaligned - both start and end in middle of byte"
- **What it tests:** Setting a range fully contained within a single byte.
- **Components:** `__bit_nset` macro
- **Notes:** Only the exact sub-byte bit range is set.

### SECTION: "setting all bits"
- **What it tests:** Setting all bits in a bitstring.
- **Components:** `__bit_nset` macro
- **Notes:** All bytes become 0xFF.

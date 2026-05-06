# test_intpack — Integer variable-length encoding/decoding tests

**File:** `test/catch2/misc_tests/test_intpack.cpp`
**Storage mode:** General
**Components under test:** `__wt_vpack_posint`, `__wt_vpack_negint`, `__wt_vpack_int`, `__wt_vunpack_posint`, `__wt_vunpack_negint`, `__wt_vunpack_int`
**Test type:** Unit

## TEST_CASE: "Byte min/max constants" [intpack]
- **What it tests:** The constants `POS_1BYTE_MAX`, `NEG_1BYTE_MAX`, etc. match their expected values for the variable-length encoding scheme.
- **Components:** Encoding constant definitions
- **Notes:** Documents the boundary values of each byte-width encoding tier.

## TEST_CASE: "Macro calculations" [intpack]
- **What it tests:** Helper macros `GET_BITS`, `WT_SIZE_CHECK_PACK`, `WT_SIZE_CHECK_UNPACK`, and `WT_LEADING_ZEROS` compute correctly for known inputs.
- **Components:** Internal encoding macros
- **Notes:** Tests macro-level arithmetic before testing the full encode/decode functions.

## TEST_CASE: "Positive integer pack and unpack" [intpack]
### SECTION: "1-byte values"
- **What it tests:** Small positive integers (0 to POS_1BYTE_MAX) encode to a single byte and decode correctly.
- **Components:** `__wt_vpack_posint`, `__wt_vunpack_posint`

### SECTION: "2-byte values"
- **What it tests:** Medium positive integers encode to 2 bytes and decode correctly.
- **Components:** `__wt_vpack_posint`, `__wt_vunpack_posint`

### SECTION: "multi-byte values (up to uint64 max)"
- **What it tests:** Large positive integers up to UINT64_MAX encode to the appropriate number of bytes and decode correctly.
- **Components:** `__wt_vpack_posint`, `__wt_vunpack_posint`

### SECTION: "round-trip for selected values"
- **What it tests:** A set of specific values from each byte-width tier round-trip correctly.
- **Components:** `__wt_vpack_posint`, `__wt_vunpack_posint`

## TEST_CASE: "Negative integer pack and unpack" [intpack]
### SECTION: "1-byte values"
- **What it tests:** Small-magnitude negative integers encode to a single byte and decode correctly.
- **Components:** `__wt_vpack_negint`, `__wt_vunpack_negint`

### SECTION: "multi-byte values"
- **What it tests:** Large-magnitude negative integers encode to multiple bytes and decode correctly.
- **Components:** `__wt_vpack_negint`, `__wt_vunpack_negint`

### SECTION: "boundary INT64_MIN"
- **What it tests:** The most negative int64_t value encodes and decodes without overflow.
- **Components:** `__wt_vpack_negint`, `__wt_vunpack_negint`

## TEST_CASE: "Signed integer pack and unpack" [intpack]
- **What it tests:** The unified `__wt_vpack_int`/`__wt_vunpack_int` API handles both positive and negative values, dispatching to the appropriate sub-functions.
- **Components:** `__wt_vpack_int`, `__wt_vunpack_int`
- **Notes:** Many sections covering: zero, positive min/max, negative min/max, INT64_MAX, INT64_MIN. The signed interface encodes sign in the leading byte.

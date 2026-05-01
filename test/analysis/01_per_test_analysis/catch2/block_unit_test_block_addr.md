# test_block_addr — Block address pack/unpack unit tests

**File:** `test/catch2/block/unit/test_block_addr.cpp`
**Storage mode:** General
**Components under test:** Block address encoding (`__wt_block_addr_pack`, `__wt_block_addr_unpack`)
**Test type:** Unit

## TEST_CASE: "Block address packing and unpacking" [block_addr]
### SECTION: "basic round-trip"
- **What it tests:** A (offset, size, checksum) triple packs to a cookie and unpacks back to identical values.
- **Components:** `__wt_block_addr_pack`, `__wt_block_addr_unpack`
- **Notes:** Standard positive values for all three fields.

### SECTION: "zero values"
- **What it tests:** Zero offset, zero size, zero checksum encode/decode without error.
- **Components:** `__wt_block_addr_pack`, `__wt_block_addr_unpack`
- **Notes:** Edge case for minimum representable address.

### SECTION: "large offset and size"
- **What it tests:** Large 64-bit offset and size values round-trip correctly.
- **Components:** `__wt_block_addr_pack`, `__wt_block_addr_unpack`
- **Notes:** Checks that the variable-length encoding handles multi-byte values.

### SECTION: "negative value behavior"
- **What it tests:** Behavior when a negative value (cast from signed integer) is passed as a field.
- **Components:** `__wt_block_addr_pack`, `__wt_block_addr_unpack`
- **Notes:** Documents the current encoding behavior for negative-valued inputs.

### SECTION: "buffer size boundary"
- **What it tests:** The packed cookie does not exceed the declared maximum buffer size.
- **Components:** `__wt_block_addr_pack`
- **Notes:** Ensures the encoding stays within `WT_BTREE_MAX_ADDR_COOKIE` bytes.

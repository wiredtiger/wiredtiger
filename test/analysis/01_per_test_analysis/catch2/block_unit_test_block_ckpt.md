# test_block_ckpt — Block checkpoint utilities: rduppo2 and blkmod entry tests

**File:** `test/catch2/block/unit/test_block_ckpt.cpp`
**Storage mode:** General
**Components under test:** `__wt_rduppo2` (round up to power of 2), `__ut_ckpt_mod_blkmod_entry`
**Test type:** Unit

## TEST_CASE: "__wt_rduppo2" [block_ckpt]
- **What it tests:** Rounds a value up to the nearest power of two multiple of a given base.
- **Components:** `__wt_rduppo2`
- **Notes:** Tests several (value, base) pairs including already-aligned values, values needing rounding, and zero.

## TEST_CASE: "__ut_ckpt_mod_blkmod_entry" [block_ckpt]
- **What it tests:** Populates a `WT_BLOCK_MODS` bitmap entry (nbits field) correctly for a given checkpoint.
- **Components:** `__ut_ckpt_mod_blkmod_entry`, `WT_BLOCK_MODS`
- **Notes:** Validates the "+1" nbits allocation introduced by WT-6366 to handle the case where the final byte of the bitmap needs an extra bit for the block that spans the file-size boundary.

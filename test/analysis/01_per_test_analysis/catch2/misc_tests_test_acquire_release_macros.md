# test_acquire_release_macros — Atomic acquire/release macro tests

**File:** `test/catch2/misc_tests/test_acquire_release_macros.cpp`
**Storage mode:** General
**Components under test:** Acquire/release atomic macros for uint64, uint32, uint16, uint8
**Test type:** Unit

## TEST_CASE: "Acquire read macros — uint64" [acquire_release]
- **What it tests:** `__wt_atomic_load_uint64_acquire` returns the stored value with acquire semantics.
- **Components:** `__wt_atomic_load_uint64_acquire`, `WT_ACQUIRE_READ_WITH_BARRIER`

## TEST_CASE: "Release write macros — uint64" [acquire_release]
- **What it tests:** `__wt_atomic_store_uint64_release` stores a value with release semantics.
- **Components:** `__wt_atomic_store_uint64_release`, `WT_RELEASE_WRITE_WITH_BARRIER`

## TEST_CASE: "Acquire read macros — uint32" [acquire_release]
- **What it tests:** `__wt_atomic_load_uint32_acquire` returns the stored value.
- **Components:** `__wt_atomic_load_uint32_acquire`

## TEST_CASE: "Release write macros — uint32" [acquire_release]
- **What it tests:** `__wt_atomic_store_uint32_release` stores a value with release semantics.
- **Components:** `__wt_atomic_store_uint32_release`

## TEST_CASE: "Acquire read macros — uint16" [acquire_release]
- **What it tests:** `__wt_atomic_load_uint16_acquire` returns the stored value.
- **Components:** `__wt_atomic_load_uint16_acquire`

## TEST_CASE: "Release write macros — uint16" [acquire_release]
- **What it tests:** `__wt_atomic_store_uint16_release` stores a value with release semantics.
- **Components:** `__wt_atomic_store_uint16_release`

## TEST_CASE: "Acquire read macros — uint8" [acquire_release]
- **What it tests:** `__wt_atomic_load_uint8_acquire` returns the stored value.
- **Components:** `__wt_atomic_load_uint8_acquire`

## TEST_CASE: "Release write macros — uint8" [acquire_release]
- **What it tests:** `__wt_atomic_store_uint8_release` stores a value with release semantics.
- **Components:** `__wt_atomic_store_uint8_release`

## TEST_CASE: "Hash-define int size workaround" [acquire_release]
- **What it tests:** Verifies that the acquire/release macros compile and function correctly for `int`-sized values via the hash-define workaround used to unify the macro family.
- **Components:** Macro expansion, integer type compatibility
- **Notes:** Addresses a C/C++ interoperability issue where `int` is not the same as `uint32_t` on some platforms.

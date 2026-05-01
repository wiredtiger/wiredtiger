# test_key_provider_header — Disagg crypt header layout and pack/validate tests

**File:** `test/catch2/ext/test_key_provider_header.cpp`
**Storage mode:** Disagg
**Components under test:** `WT_CRYPT_HEADER` struct layout, `__ut_disagg_set_crypt_header`, `__ut_disagg_validate_crypt`
**Test type:** Unit

## TEST_CASE_METHOD (kp_header_fixture): "WT_CRYPT_HEADER offset validation" [key_provider_header]
- **What it tests:** The `WT_CRYPT_HEADER` struct is exactly 16 bytes and each field is at the expected byte offset.
- **Components:** `WT_CRYPT_HEADER`
- **Notes:** Verifies: `version` at offset 0, `compatible_version` at offset 2, `size` at offset 4, `checksum` at offset 8. ABI stability check.

## TEST_CASE_METHOD (kp_header_fixture): "Set crypt header (__ut_disagg_set_crypt_header)" [key_provider_header]
- **What it tests:** `__ut_disagg_set_crypt_header` correctly packs version, compatible_version, size, and checksum into the header byte sequence.
- **Components:** `__ut_disagg_set_crypt_header`
- **Notes:** After the call, each field at its known offset matches the expected value. Effectively tests the header serialization path.

## TEST_CASE_METHOD (kp_header_fixture): "Validate crypt header (__ut_disagg_validate_crypt)" [key_provider_header]
### SECTION: "valid header"
- **What it tests:** A correctly formed `WT_CRYPT_HEADER` passes validation and returns 0.
- **Components:** `__ut_disagg_validate_crypt`
- **Notes:** Normal success case.

### SECTION: "wrong version"
- **What it tests:** A header with an unsupported `version` field returns ENOTSUP.
- **Components:** `__ut_disagg_validate_crypt`
- **Notes:** Version mismatch detection.

### SECTION: "incompatible compatible_version"
- **What it tests:** A header whose `compatible_version` exceeds the current software version returns ENOTSUP.
- **Components:** `__ut_disagg_validate_crypt`
- **Notes:** Forward-compatibility guard.

### SECTION: "wrong size"
- **What it tests:** A header whose declared size does not match the actual buffer size returns EINVAL.
- **Components:** `__ut_disagg_validate_crypt`
- **Notes:** Prevents reading truncated or over-sized encrypted payloads.

### SECTION: "checksum mismatch"
- **What it tests:** A header with a corrupted checksum field returns EINVAL.
- **Components:** `__ut_disagg_validate_crypt`
- **Notes:** Data integrity check via CRC.

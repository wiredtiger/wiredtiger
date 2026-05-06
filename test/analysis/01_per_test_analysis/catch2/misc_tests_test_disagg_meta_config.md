# test_disagg_meta_config — Disaggregated storage metadata parsing tests

**File:** `test/catch2/misc_tests/test_disagg_meta_config.cpp`
**Storage mode:** Disagg
**Components under test:** `__wt_disagg_parse_meta`, `__wti_disagg_parse_crypt_meta`, `__ut_disagg_parse_version_and_check`
**Test type:** Unit

## TEST_CASE_METHOD (disagg_fixture): "Parse metadata" [disagg]
### SECTION: "All fields present"
- **What it tests:** A metadata string containing `checkpoint`, `timestamp`, and `key_provider` fields is parsed correctly into a `WT_DISAGG_METADATA` struct.
- **Components:** `__wt_disagg_parse_meta`, `WT_DISAGG_METADATA`
- **Notes:** Verifies checkpoint string, timestamp (hex value `c0ffee12`), and key_provider string are all correctly extracted.

### SECTION: "Key provider missing"
- **What it tests:** A metadata string without the `key_provider` field parses successfully; `key_provider` and `key_provider_len` in the result are null/0.
- **Components:** `__wt_disagg_parse_meta`
- **Notes:** `key_provider` is optional.

### SECTION: "Missing fields"
- **What it tests:** A metadata string missing the required `timestamp` field returns EINVAL.
- **Components:** `__wt_disagg_parse_meta`
- **Notes:** `timestamp` is mandatory.

### SECTION: "Null metadata"
- **What it tests:** Passing a `WT_ITEM` with null data pointer returns EINVAL.
- **Components:** `__wt_disagg_parse_meta`
- **Notes:** Null-pointer guard.

### SECTION: "Empty metadata"
- **What it tests:** A zero-length metadata buffer returns EINVAL.
- **Components:** `__wt_disagg_parse_meta`
- **Notes:** Empty-input guard.

### SECTION: "Length limited"
- **What it tests:** When `metadata_buf.size` is smaller than the full string length, parsing respects the size limit (truncates cleanly).
- **Components:** `__wt_disagg_parse_meta`
- **Notes:** Timestamp `c0ffee12` truncated to `c0ffee` — parsed value is `0xc0ffee`.

### SECTION: "Unknown keys ignored if version doesn't match"
- **What it tests:** When the metadata version exceeds `WT_DISAGG_CHECKPOINT_TURTLE_VERSION`, unknown keys do not cause an error.
- **Components:** `__wt_disagg_parse_meta`, versioned metadata
- **Notes:** Forward-compatibility: newer metadata may have keys unknown to this binary, but if `compatible_version <= current`, parsing succeeds.

### SECTION: "Unknown keys are an error if version matches"
- **What it tests:** When the metadata version exactly matches `WT_DISAGG_CHECKPOINT_TURTLE_VERSION`, unknown keys return EINVAL.
- **Components:** `__wt_disagg_parse_meta`
- **Notes:** Current-version metadata must not contain unknown keys.

## TEST_CASE_METHOD (disagg_fixture): "Parse crypt key metadata" [disagg]
### SECTION: "Well-formed"
- **What it tests:** A valid key provider config `(page.1=(page_id=1,lsn=123),version=1)` is parsed and returns `page_id=1, lsn=123`.
- **Components:** `__wti_disagg_parse_crypt_meta`
- **Notes:** Tests the happy path of the crypt metadata parser.

### SECTION: "Malformed"
- **What it tests:** Eight malformed inputs each return EINVAL: invalid page_id, out-of-range page_id, invalid lsn, missing lsn, missing page_id, missing version, unsupported version, completely invalid format.
- **Components:** `__wti_disagg_parse_crypt_meta`
- **Notes:** Exhaustive error-path coverage.

## TEST_CASE_METHOD (disagg_fixture): "Legacy metadata format" [disagg]
### SECTION: "Complete metadata"
- **What it tests:** The legacy newline-separated format (`checkpoint_string\ntimestamp=hex`) is parsed correctly.
- **Components:** `__wt_disagg_parse_meta`
- **Notes:** Backward compatibility with older turtle file format.

### SECTION: "Length limited"
- **What it tests:** A legacy format buffer truncated by 2 bytes parses with a correspondingly truncated timestamp.
- **Components:** `__wt_disagg_parse_meta`
- **Notes:** `c0ffee12` truncated to `c0ffee`.

### SECTION: "Missing timestamp"
- **What it tests:** Legacy format with no `\ntimestamp=...` line returns EINVAL.
- **Components:** `__wt_disagg_parse_meta`
- **Notes:** Timestamp is required even in legacy format.

### SECTION: "Missing timestamp 2"
- **What it tests:** Legacy format ending with `\n` but no timestamp value returns EINVAL.
- **Components:** `__wt_disagg_parse_meta`
- **Notes:** Trailing newline without timestamp content.

### SECTION: "Invalid timestamp"
- **What it tests:** Five invalid timestamp forms each return EINVAL: empty value, non-hex characters, negative number, too-large number, misspelled key.
- **Components:** `__wt_disagg_parse_meta`
- **Notes:** Exhaustive timestamp validation error paths.

## TEST_CASE_METHOD (disagg_fixture): "Parse metadata with version" [disagg]
### SECTION: "Valid version"
- **What it tests:** `__ut_disagg_parse_version_and_check` returns 0 and populates `version=1, compatible_version=1`.
- **Components:** `__ut_disagg_parse_version_and_check`
- **Notes:** Standard version fields.

### SECTION: "Incompatible version"
- **What it tests:** A `compatible_version=999` that exceeds the software version returns ENOTSUP.
- **Components:** `__ut_disagg_parse_version_and_check`
- **Notes:** Forward incompatibility.

### SECTION: "Missing version"
- **What it tests:** A string missing the `version` field returns EINVAL.
- **Components:** `__ut_disagg_parse_version_and_check`
- **Notes:** `version` is required when any versioning fields are present.

### SECTION: "Missing compatible_version"
- **What it tests:** A string missing `compatible_version` returns EINVAL.
- **Components:** `__ut_disagg_parse_version_and_check`
- **Notes:** `compatible_version` is required when any versioning fields are present.

### SECTION: "Default version when omitted"
- **What it tests:** A string with neither `version` nor `compatible_version` returns 0 with both fields set to `WT_DISAGG_CHECKPOINT_TURTLE_VERSION_DEFAULT`.
- **Components:** `__ut_disagg_parse_version_and_check`, `WT_DISAGG_CHECKPOINT_TURTLE_VERSION_DEFAULT`
- **Notes:** Handles the legacy case where no version fields are written.

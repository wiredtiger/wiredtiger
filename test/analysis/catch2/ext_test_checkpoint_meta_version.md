# test_checkpoint_meta_version — Disagg checkpoint metadata version validation tests

**File:** `test/catch2/ext/test_checkpoint_meta_version.cpp`
**Storage mode:** Disagg
**Components under test:** `__ut_disagg_validate_checkpoint_meta_version`, checkpoint metadata versioning (`WT_DISAGG_CHECKPOINT_META_VERSION`)
**Test type:** Unit

## TEST_CASE_METHOD (checkpoint_meta_version_fixture): "Parse checkpoint metadata version" [disagg_checkpoint_meta_version]
### SECTION: "version 1/1 — fully compatible"
- **What it tests:** A metadata string with `version=1,compatible_version=1` is accepted and fields are populated.
- **Components:** `__ut_disagg_validate_checkpoint_meta_version`
- **Notes:** Normal case; both version and compatible_version equal 1.

### SECTION: "backward compatible — version higher, compatible_version=1"
- **What it tests:** A newer metadata format that declares itself backward-compatible with version 1 is accepted.
- **Components:** `__ut_disagg_validate_checkpoint_meta_version`
- **Notes:** Compatible older readers should be able to parse newer metadata.

### SECTION: "version only — missing compatible_version"
- **What it tests:** A metadata string missing `compatible_version` returns EINVAL.
- **Components:** `__ut_disagg_validate_checkpoint_meta_version`
- **Notes:** Both fields are required.

### SECTION: "compatible_version only — missing version"
- **What it tests:** A metadata string missing `version` returns EINVAL.
- **Components:** `__ut_disagg_validate_checkpoint_meta_version`
- **Notes:** Both fields are required.

### SECTION: "forward compatibility error (ENOTSUP)"
- **What it tests:** A metadata format whose `compatible_version` exceeds the current software version returns ENOTSUP.
- **Components:** `__ut_disagg_validate_checkpoint_meta_version`
- **Notes:** Signals that this binary cannot safely read the metadata.

### SECTION: "illegal config — compatible_version > version"
- **What it tests:** A metadata string where `compatible_version > version` returns EINVAL.
- **Components:** `__ut_disagg_validate_checkpoint_meta_version`
- **Notes:** `compatible_version` must always be <= `version`.

### SECTION: "multiple incompatible versions"
- **What it tests:** Several combinations of (version, compatible_version) that should all return ENOTSUP are verified.
- **Components:** `__ut_disagg_validate_checkpoint_meta_version`
- **Notes:** Exhaustive forward-incompatibility matrix.

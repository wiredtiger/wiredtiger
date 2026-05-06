# test_config — Config decimal integer parsing tests

**File:** `test/catch2/misc_tests/test_config.cpp`
**Storage mode:** General
**Components under test:** `__wti_config_parse_dec`
**Test type:** Unit

## TEST_CASE: "Config decimal integer parsing" [config]
### SECTION: "no conversion"
- **What it tests:** An empty string returns 0 without error.
- **Components:** `__wti_config_parse_dec`
- **Notes:** Baseline / empty input case.

### SECTION: "boundary — INT64_MAX and INT64_MIN"
- **What it tests:** The maximum and minimum int64_t values parse without overflow.
- **Components:** `__wti_config_parse_dec`
- **Notes:** Edge values at the type boundary.

### SECTION: "boundary ± 1"
- **What it tests:** INT64_MAX + 1 and INT64_MIN - 1 are detected as out-of-range.
- **Components:** `__wti_config_parse_dec`
- **Notes:** Just-beyond-boundary overflow detection.

### SECTION: "out of range (ERANGE)"
- **What it tests:** Numbers larger than INT64_MAX or smaller than INT64_MIN return ERANGE.
- **Components:** `__wti_config_parse_dec`
- **Notes:** Several large-magnitude values verified.

### SECTION: "limited length"
- **What it tests:** Parsing stops at a specified length limit, treating the truncated substring as the number.
- **Components:** `__wti_config_parse_dec`
- **Notes:** Used when config strings are not null-terminated.

### SECTION: "non-digit stop, positive/negative signs"
- **What it tests:** Parsing stops at the first non-digit character; a leading `+` or `-` sign is handled correctly.
- **Components:** `__wti_config_parse_dec`
- **Notes:** Verifies that trailing non-digit characters are ignored.

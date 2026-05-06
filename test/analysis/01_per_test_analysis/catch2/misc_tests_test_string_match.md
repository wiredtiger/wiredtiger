# test_string_match — String matching macro tests

**File:** `test/catch2/misc_tests/test_string_match.cpp`
**Storage mode:** General
**Components under test:** `WT_STRING_MATCH`, `WT_STRING_LIT_MATCH`, `WT_CONFIG_MATCH`, `WT_CONFIG_LIT_MATCH`
**Test type:** Unit

## TEST_CASE: "String matching macros" [string_match]
### SECTION: "null-terminated string"
- **What it tests:** `WT_STRING_MATCH` correctly matches/does-not-match against the string `"green"` and its substrings, an empty string, and a null string. `WT_STRING_LIT_MATCH` and `WT_CONFIG_MATCH`/`WT_CONFIG_LIT_MATCH` are also verified against the same inputs.
- **Components:** `WT_STRING_MATCH`, `WT_STRING_LIT_MATCH`, `WT_CONFIG_MATCH`, `WT_CONFIG_LIT_MATCH`
- **Notes:** Tests: exact match, prefix match, suffix match, substring match, no-match, empty string match, null string behavior.

### SECTION: "non-null-terminated string"
- **What it tests:** The same matching behavior when the target string is not null-terminated (length is passed explicitly).
- **Components:** `WT_STRING_MATCH`, `WT_STRING_LIT_MATCH`, `WT_CONFIG_MATCH`, `WT_CONFIG_LIT_MATCH`
- **Notes:** WiredTiger config strings are often length-delimited rather than null-terminated; this section ensures the macros handle that correctly.

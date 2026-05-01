# test_sub_level_error_session_set_last_error — Session set/reset last error function tests

**File:** `test/catch2/sub_level_error/unit/test_sub_level_error_session_set_last_error.cpp`
**Storage mode:** General
**Components under test:** `__wt_session_set_last_error`, `__wt_session_reset_last_error`, `WT_ERROR_INFO`
**Test type:** Unit

## TEST_CASE: "Test set_last_error and reset_last_error functions" [sub_level_error_session_set_last_error, sub_level_error]
### SECTION: "Test with NULL session"
- **What it tests:** `__wt_session_reset_last_error(NULL)` does not crash (null pointer guard).
- **Components:** `__wt_session_reset_last_error`

### SECTION: "Test with initial values"
- **What it tests:** After `__wt_session_reset_last_error`, `err_info` contains `err=0, sub_level_err=WT_NONE, err_msg=WT_ERROR_INFO_SUCCESS`.
- **Components:** `__wt_session_reset_last_error`

### SECTION: "Test with EINVAL error"
- **What it tests:** `__wt_session_set_last_error(session, EINVAL, WT_BACKGROUND_COMPACT_ALREADY_RUNNING, msg)` stores all three fields correctly.
- **Components:** `__wt_session_set_last_error`

### SECTION: "Test overwriting/resetting the error message"
- **What it tests:** A second call to `__wt_session_set_last_error` does not overwrite an already-set error. After `__wt_session_reset_last_error`, the struct is cleared to defaults.
- **Components:** `__wt_session_set_last_error`, `__wt_session_reset_last_error`
- **Notes:** The first error set is preserved until explicitly reset.

### SECTION: "Test with multiple errors (varying err/sub_level_err/err_msg)"
- **What it tests:** A sequence of set → reset → set → reset operations correctly cycles through different error codes (`WT_BACKGROUND_COMPACT_ALREADY_RUNNING`, `WT_UNCOMMITTED_DATA`, `WT_DIRTY_DATA`).
- **Components:** `__wt_session_set_last_error`, `__wt_session_reset_last_error`

### SECTION: "Test with large error message"
- **What it tests:** A 1024-character error message is stored without truncation or buffer overflow.
- **Components:** `__wt_session_set_last_error`
- **Notes:** Validates the error message buffer size.

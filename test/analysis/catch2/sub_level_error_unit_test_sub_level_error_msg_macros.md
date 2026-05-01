# test_sub_level_error_msg_macros — WT_RET_SUB, WT_ERR_SUB, WT_RET_MSG, WT_ERR_MSG macro tests

**File:** `test/catch2/sub_level_error/unit/test_sub_level_error_msg_macros.cpp`
**Storage mode:** General
**Components under test:** `WT_RET_SUB`, `WT_ERR_SUB`, `WT_RET_MSG`, `WT_ERR_MSG`
**Test type:** Unit

## TEST_CASE: "Test WT_RET_SUB, WT_ERR_SUB, WT_RET_MSG, WT_ERR_MSG" [sub_level_error_msg_macros, sub_level_error]
### SECTION: "Test WT_RET_SUB with EINVAL error WT_BACKGROUND_COMPACT_ALREADY_RUNNING sub_level_error"
- **What it tests:** `WT_RET_SUB(session, EINVAL, WT_BACKGROUND_COMPACT_ALREADY_RUNNING, msg)` returns EINVAL and stores the correct error, sub-level error, and message in `err_info`.
- **Components:** `WT_RET_SUB`, `__wt_session_set_last_error`
- **Notes:** `WT_RET_SUB` sets the sub-level error before returning.

### SECTION: "Test WT_ERR_SUB with EINVAL error WT_BACKGROUND_COMPACT_ALREADY_RUNNING sub_level_error"
- **What it tests:** `WT_ERR_SUB(session, EINVAL, WT_BACKGROUND_COMPACT_ALREADY_RUNNING, msg)` jumps to the `err:` label and returns EINVAL with the sub-level error stored.
- **Components:** `WT_ERR_SUB`, `__wt_session_set_last_error`
- **Notes:** `WT_ERR_SUB` sets the sub-level error and jumps to the error label.

### SECTION: "Test WT_RET_MSG with EINVAL error"
- **What it tests:** `WT_RET_MSG(session, EINVAL, msg)` returns EINVAL with `sub_level_err=WT_NONE` and the provided message.
- **Components:** `WT_RET_MSG`
- **Notes:** `WT_RET_MSG` does not set a sub-level error; only the message is stored.

### SECTION: "Test WT_ERR_MSG with EINVAL error"
- **What it tests:** `WT_ERR_MSG(session, EINVAL, msg)` jumps to the `err:` label and returns EINVAL with `sub_level_err=WT_NONE` and the message.
- **Components:** `WT_ERR_MSG`
- **Notes:** `WT_ERR_MSG` does not set a sub-level error.

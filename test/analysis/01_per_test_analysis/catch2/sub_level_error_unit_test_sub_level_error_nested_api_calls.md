# test_sub_level_error_nested_api_calls — Nested API call error recording tests

**File:** `test/catch2/sub_level_error/unit/test_sub_level_error_nested_api_calls.cpp`
**Storage mode:** General
**Components under test:** `API_END_RET`, `CURSOR_API_CALL`, `SESSION_API_CALL_NOCONF`, `WT_ERR_NOTFOUND_OK`, `__wt_session_set_last_error`
**Test type:** Unit

## TEST_CASE: "API_END_RET nested - test that nested API calls only keep explicitly set errors" [sub_level_error_nested_api_calls, sub_level_error]

Tests how `err_info` is propagated when nested API calls return errors. Uses a helper that calls an inner cursor API returning `WT_NOTFOUND`, then optionally raises a second error.

### SECTION: "Test nested API call with WT_NOTFOUND inside WT_ERR_NOTFOUND_OK()"
- **What it tests:** `WT_NOTFOUND` swallowed by `WT_ERR_NOTFOUND_OK` results in overall return 0 and cleared `err_info`.
- **Notes:** Error info is `(err=0, WT_NONE, WT_ERROR_INFO_SUCCESS)`.

### SECTION: "Test nested API call with WT_NOTFOUND inside WT_ERR_NOTFOUND_OK(), followed by EINVAL"
- **What it tests:** `WT_NOTFOUND` is swallowed, then EINVAL is raised later; final error info reflects EINVAL with message "Something was invalid".
- **Notes:** Inner error is suppressed; outer error wins.

### SECTION: "Test nested API call with WT_NOTFOUND inside WT_ERR()"
- **What it tests:** `WT_NOTFOUND` not swallowed results in overall `WT_NOTFOUND` and `err_info` with `WT_ERROR_INFO_EMPTY` message (no explicit set).
- **Notes:** Without explicit `__wt_session_set_last_error`, the message is empty.

### SECTION: "Test nested API call with WT_NOTFOUND inside WT_ERR(), followed by EINVAL"
- **What it tests:** `WT_NOTFOUND` stops execution; EINVAL is never reached; final result is `WT_NOTFOUND` with empty message.
- **Notes:** First error wins; later errors are not reached.

### SECTION: "Test nested API call with WT_NOTFOUND set explicitly inside WT_ERR_NOTFOUND_OK()"
- **What it tests:** Inner call explicitly sets `err_info` before `WT_NOTFOUND` is swallowed; outer call returns 0 and clears `err_info` on success.
- **Notes:** Success clears even explicitly-set inner errors.

### SECTION: "Test nested API call with WT_NOTFOUND set explicitly inside WT_ERR_NOTFOUND_OK(), followed by EINVAL"
- **What it tests:** Inner sets `err_info` with "Something was not found"; outer later sets "Something was invalid"; final result is the inner message preserved (first explicit set wins).
- **Notes:** `err_info` is only overwritten if not already set.

### SECTION: "Test nested API call with WT_NOTFOUND set explicitly inside WT_ERR()"
- **What it tests:** Inner explicitly sets `err_info` with "Something was not found"; outer sees `WT_NOTFOUND` and preserves the inner message.
- **Notes:** Explicitly-set error info from inner call survives to the caller.

### SECTION: "Test nested API call with WT_NOTFOUND set explicitly inside WT_ERR(), followed by EINVAL"
- **What it tests:** Inner sets `err_info`; `WT_NOTFOUND` stops execution; EINVAL is not reached; inner message is preserved.
- **Notes:** First explicit set wins; later potential sets are skipped.

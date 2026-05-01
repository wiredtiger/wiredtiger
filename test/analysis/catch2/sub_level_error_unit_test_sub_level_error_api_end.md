# test_sub_level_error_api_end — API_END_RET and TXN_API_END error recording tests

**File:** `test/catch2/sub_level_error/unit/test_sub_level_error_api_end.cpp`
**Storage mode:** General
**Components under test:** `API_END_RET`, `TXN_API_END`, `__wt_session_set_last_error`, `WT_ERROR_INFO`
**Test type:** Unit

## TEST_CASE: "API_END_RET and TXN_API_END record error info correctly" [sub_level_error_api_end, sub_level_error]
### SECTION: "API_END_RET with success (ret=0)"
- **What it tests:** When `API_END_RET` is called with `ret=0`, the `err_info` struct is cleared to defaults (`err=0, sub_level_err=WT_NONE, err_msg=WT_ERROR_INFO_SUCCESS`).
- **Components:** `API_END_RET`

### SECTION: "API_END_RET with EINVAL"
- **What it tests:** When `ret=EINVAL` and no explicit `__wt_session_set_last_error` was called, `API_END_RET` stores a default sub-level error and empty message.
- **Components:** `API_END_RET`

### SECTION: "API_END_RET with explicitly set error info"
- **What it tests:** When `__wt_session_set_last_error` is called before `API_END_RET`, the error info is preserved exactly.
- **Components:** `API_END_RET`, `__wt_session_set_last_error`

### SECTION: "TXN_API_END with success"
- **What it tests:** `TXN_API_END` with `ret=0` clears the error info to defaults.
- **Components:** `TXN_API_END`

### SECTION: "TXN_API_END with error"
- **What it tests:** `TXN_API_END` with `ret=EINVAL` stores the error code and preserves any explicitly-set sub-level error.
- **Components:** `TXN_API_END`

### SECTION: "API_END_RET with WT_ROLLBACK"
- **What it tests:** `WT_ROLLBACK` is treated as a regular error by `API_END_RET`; sub-level info is preserved.
- **Components:** `API_END_RET`

### SECTION: "API_END_RET with WT_NOTFOUND"
- **What it tests:** `WT_NOTFOUND` results in `err_info` with an empty message (`WT_ERROR_INFO_EMPTY`).
- **Components:** `API_END_RET`

### SECTION: "API_END_RET does not overwrite existing error with same error"
- **What it tests:** If `err_info` already has `err=EINVAL` and `API_END_RET` is called again with `ret=EINVAL`, the existing message is preserved.
- **Components:** `API_END_RET`

### SECTION: "Nested API_END_RET preserves first error"
- **What it tests:** When a nested API call sets an error and the outer `API_END_RET` sees the same error code, the inner error's message and sub-level code are preserved.
- **Components:** `API_END_RET`, nested API calls

### SECTION: "API_END_RET with WT_PANIC"
- **What it tests:** `WT_PANIC` errors are handled by `API_END_RET`; error info reflects the panic.
- **Components:** `API_END_RET`

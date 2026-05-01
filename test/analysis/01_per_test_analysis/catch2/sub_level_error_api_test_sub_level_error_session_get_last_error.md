# test_sub_level_error_session_get_last_error — session->get_last_error() API tests

**File:** `test/catch2/sub_level_error/api/test_sub_level_error_session_get_last_error.cpp`
**Storage mode:** General
**Components under test:** `session->get_last_error()`, `WT_ERROR_INFO`
**Test type:** API contract

## TEST_CASE: "Test session->get_last_error() API" [sub_level_error_session_get_last_error, sub_level_error]
### SECTION: "default values"
- **What it tests:** After opening a fresh session, `get_last_error()` returns the default `WT_ERROR_INFO` values: `err=0`, `sub_level_err=WT_NONE`, `err_msg=WT_ERROR_INFO_SUCCESS`.
- **Components:** `session->get_last_error`, `WT_ERROR_INFO`, `WT_NONE`, `WT_ERROR_INFO_SUCCESS`
- **Notes:** Verifies the initial state of the error info struct before any errors have occurred.

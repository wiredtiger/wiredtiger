# test_page_log_handle — Disagg page log handle lifecycle and config tests

**File:** `test/catch2/misc_tests/test_page_log_handle.cpp`
**Storage mode:** Disagg
**Components under test:** `__wti_disagg_conn_config`, `__wti_disagg_destroy`, `WT_PAGE_LOG`, `WT_PAGE_LOG_HANDLE`
**Test type:** Unit

## TEST_CASE: "Test disaggregated configuration logic" [disagg_config]
### SECTION: "Test page log handle is constructed"
- **What it tests:** `__wti_disagg_conn_config` opens a page log handle for metadata (`page_log_meta`) when a page log extension is configured. Returns EINVAL (because the mock doesn't complete the full config), but the handle pointer is non-null.
- **Components:** `__wti_disagg_conn_config`, `disaggregated_storage.page_log`, `disaggregated_storage.npage_log`, `disaggregated_storage.page_log_meta`
- **Notes:** `page_log_key_provider` remains null because no key provider is configured.

### SECTION: "Test key provider handle is constructed"
- **What it tests:** When `conn_impl->key_provider` is set to a non-null value, `__wti_disagg_conn_config` also constructs a `page_log_key_provider` handle.
- **Components:** `__wti_disagg_conn_config`, `disaggregated_storage.page_log_key_provider`, `WT_KEY_PROVIDER`
- **Notes:** The key provider page log handle is only allocated when a key provider extension is present.

### SECTION: "Test key provider and page log handle is destroyed"
- **What it tests:** `__wti_disagg_destroy` calls `plh_close` on both the metadata and key provider page log handles, and nulls out the pointers.
- **Components:** `__wti_disagg_destroy`, `plh_close` callback
- **Notes:** Uses a stack-allocated mock `WT_PAGE_LOG_HANDLE` with a no-op `mock_plh_close`. After destroy, both `page_log_meta` and `page_log_key_provider` are null.

# test_sub_level_error_compact — Sub-level error handling in compaction workflows

**File:** `test/catch2/sub_level_error/unit/test_sub_level_error_compact.cpp`
**Storage mode:** General
**Components under test:** `__wt_background_compact_signal`, `WT_ERROR_INFO`, `WT_BACKGROUND_COMPACT_ALREADY_RUNNING`
**Test type:** Unit

## TEST_CASE: "Test functions for error handling in compaction workflows" [sub_level_error_compact, sub_level_error]
### SECTION: "Test __wt_background_compact_signal - in-memory or readonly database"
- **What it tests:** Calling `__wt_background_compact_signal` on an in-memory or read-only database returns `ENOTSUP` and leaves `err_info` at defaults (`err=0, sub_level_err=WT_NONE`).
- **Components:** `__wt_background_compact_signal`, `WT_CONN_IN_MEMORY`, `WT_CONN_READONLY`
- **Notes:** No sub-level error is set for this expected-failure case.

### SECTION: "Test __wt_background_compact_signal - changes in config string"
- **What it tests:**
  - Config string without a `background` key returns `WT_NOTFOUND` with no sub-level error.
  - `background=false` returns 0 with no sub-level error.
  - `background=true` returns 0 with no sub-level error.
- **Components:** `__wt_background_compact_signal`, config parsing
- **Notes:** These are normal (non-error) config transitions.

### SECTION: "Test __wt_background_compact_signal - background_compact configuration"
- **What it tests:**
  - If compaction is already running and the new config matches the current config, the call returns 0 with no sub-level error.
  - If compaction is already running but the new config does not match, the call returns `EINVAL` with `sub_level_err=WT_BACKGROUND_COMPACT_ALREADY_RUNNING` and the message "Cannot reconfigure background compaction while it's already running."
- **Components:** `__wt_background_compact_signal`, `WT_BACKGROUND_COMPACT_ALREADY_RUNNING`
- **Notes:** The `WT_BACKGROUND_COMPACT_ALREADY_RUNNING` sub-level error is the specific signal for this conflict.

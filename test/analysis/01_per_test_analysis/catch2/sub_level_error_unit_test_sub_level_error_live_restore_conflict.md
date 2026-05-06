# test_sub_level_error_live_restore_conflict — Sub-level error for live restore conflicts

**File:** `test/catch2/sub_level_error/unit/test_sub_level_error_live_restore_conflict.cpp`
**Storage mode:** General (Live Restore)
**Components under test:** `session->open_cursor("backup:")`, `WT_CONFLICT_LIVE_RESTORE`
**Test type:** Unit

## TEST_CASE: "Test WT_CONFLICT_LIVE_RESTORE" [sub_level_error_live_restore_conflict, sub_level_error, live_restore]
### SECTION: "Test WT_CONFLICT_LIVE_RESTORE while opening backup cursor"
- **What it tests:** Opening a backup cursor (`backup:`) when live restore is active returns `EINVAL` with `sub_level_err=WT_CONFLICT_LIVE_RESTORE` and the message "backup cannot be taken when live restore is enabled".
- **Components:** `session->open_cursor`, `WT_CONFLICT_LIVE_RESTORE`, `live_restore_test_env`
- **Notes:** Live restore and backup are mutually exclusive. The `live_restore_test_env` fixture sets up the connection with live restore enabled. The cursor pointer is verified to be null after the failed open.

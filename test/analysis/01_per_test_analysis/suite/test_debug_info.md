# test_debug_info — Tests WT_CONNECTION::debug_info diagnostics output

**File:** `test/suite/test_debug_info.py`
**Storage mode:** General
**Components under test:** connection diagnostics, cursor state, session state, backup

## Test Cases

### `test_debug_info.test_debug`
- **What it tests:** Exercises all documented categories of `WT_CONNECTION::debug_info` and verifies the expected diagnostic strings appear in stdout for handles, sessions, positioned cursors, special cursor types, and backup state.
- **Components:** `src/conn/conn_debug.c`, `src/cursor/`, `src/backup/`
- **Notes:** Uses `expectedStdoutPattern` to verify output. Covers the special cursor URIs `backup:`, `log:`, `metadata:`, and `statistics:`. Also covers incremental backup cursor (`granularity=4k`, `this_id="ID1"`). Log is enabled; connection config uses `statistics=(fast)`.

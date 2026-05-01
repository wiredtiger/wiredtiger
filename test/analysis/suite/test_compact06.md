# test_compact06 — Background compaction API validation and run_once behavior

**File:** `test/suite/test_compact06.py`
**Storage mode:** General (skips tiered)
**Components under test:** background compaction server, API validation, run_once mode

## Test Cases

### `test_compact06.test_background_compact_api`
- **What it tests:** Validates background compaction API restrictions and the `run_once` behavior. Tests that: (1) background=true cannot be called on a specific URI; (2) other configs cannot be set alongside background=false; (3) only "table:" URIs are valid in the exclude list; (4) an already-running server cannot be reconfigured (returns `WT_BACKGROUND_COMPACT_ALREADY_RUNNING`); (5) `run_once=true` causes the server to stop itself after one pass; (6) HS file is always skipped (too small).
- **Components:** `src/session/session_compact.c`, `src/support/background_compact.c`
- **Notes:** Skip: tiered. Tests error messages precisely with `assertRaisesWithMessage`. Uses `get_bg_compaction_files_skipped()` polling loop to detect HS skip. Verifies cumulative skip count increments correctly across multiple enable/disable cycles. Uses `debug_mode=(background_compact)` for fine-grained control.

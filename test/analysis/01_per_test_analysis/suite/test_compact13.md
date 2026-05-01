# test_compact13 — Background compaction statistics reset after disable

**File:** `test/suite/test_compact13.py`
**Storage mode:** General (skips tiered)
**Components under test:** background compaction server, statistics lifecycle

## Test Cases

### `test_compact13.test_compact13`
- **What it tests:** Verifies that background compaction statistics are correctly reset after the server is disabled and re-enabled. After the first run (no deletions, nothing to compact — all files skipped), disables the server, deletes 90% of data, re-enables, and verifies that both tables are compacted in the second run.
- **Components:** `src/support/background_compact.c`
- **Notes:** Skip: tiered. Creates 2 tables with 100 000 rows. First background pass: waits for `files_skipped >= n_tables + 1` (includes HS), then disables. After deletion, second background pass: waits for `files_compacted >= 2`. Tests that the transition from "nothing to compact" to "data to compact" correctly triggers work in the second run, verifying state reset on disable.

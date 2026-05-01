# test_cc10 — Checkpoint cleanup thread runs at configurable intervals

**File:** `test/suite/test_cc10.py`
**Storage mode:** General
**Components under test:** checkpoint cleanup subsystem, thread scheduling, statistics

## Test Cases

### `test_cc10.test_cc10`
- **What it tests:** Verifies that the CC background thread runs periodically according to `checkpoint_cleanup=[wait=N,file_wait_ms=M]` configuration and that it correctly cleans up obsolete HS content without relying on the debug force-cleanup option.
- **Components:** `src/conn/conn_sweep.c`, `src/history/`
- **Notes:** Four scenarios varying `wait` (1–3 s) and `file_wait_ms` (0–3 000 ms). Populates 1 000 rows at ts=1, updates all at ts=10, advances oldest to ts=10 (making ts=1 HS entries obsolete), then sleeps 5 seconds to allow the background thread to fire naturally before calling `wait_for_cc_to_run()`. Asserts `pages_visited > 0` and `pages_evict + pages_removed > 0`. Verbose `checkpoint_cleanup:1` output is suppressed via `ignoreStdoutPattern('WT_VERB_CHECKPOINT_CLEANUP')`.

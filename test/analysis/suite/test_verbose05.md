# test_verbose05 — Checkpoint progress verbose logging: intermediate progress message frequency

**File:** `test/suite/test_verbose05.py`
**Storage mode:** General (skipped for disagg and tiered)
**Components under test:** `verbose=[checkpoint_progress:0]`, checkpoint progress logging, backoff thresholds

## Test Cases

### `test_verbose05.test_checkpoint_progress_log_count`
- **What it tests:** Creates a table and populates it with 100 or 200,000 rows (4KB long values to fill pages); runs a checkpoint with `verbose=[checkpoint_progress:0]`; reads stdout and counts lines matching the pattern `"Checkpoint has been running for N seconds, wrote N pages (N MB), walked N pages and checkpointed N files"`; asserts the count is between `log10(reconciled_pages)` and `10 * log10(reconciled_pages)`, verifying that progress messages are emitted at an appropriate logarithmic rate (not too few, not too many).
- **Components:** `checkpoint.c`, `verbose.c`
- **Notes:** Skipped for disagg (FIXME-WT-?) and tiered. Parameterized over small_db (100 rows) and large_db (200,000 rows). The upper bound guards against excessive logging; the lower bound ensures at least some progress messages are emitted. Uses `statistics=(all)` to read `checkpoint_pages_reconciled` as an upper bound estimate.

# test_compact05 — Foreground compaction proceeds only when free space exceeds free_space_target

**File:** `test/suite/test_compact05.py`
**Storage mode:** General (skips tiered)
**Components under test:** compaction subsystem, free_space_target threshold, statistics

## Test Cases

### `test_compact05.test_compact05`
- **What it tests:** Verifies that foreground compaction only proceeds when the available free space exceeds the configured `free_space_target` threshold. When the threshold is below available bytes (1 MB), compaction runs and rewrites pages. When the threshold exceeds available bytes (45 MB), compaction logs a message and does no work.
- **Components:** `src/block/block_compact.c`, `src/session/session_compact.c`
- **Notes:** Skip: tiered. Two scenarios: `free_space_target=1MB` (expected_compaction=True) and `free_space_target=45MB` (expected_compaction=False). Deletes 4 ranges of 10 000 keys from a 100 000-row table to create free space. For expected failure, uses `expectedStdoutPattern('number of available bytes.*is less than the configured threshold')`. Asserts `pages_rewritten > 0` and `pages_rewritten_expected > 0` on success; both equal 0 on failure.

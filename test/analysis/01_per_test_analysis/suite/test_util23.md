# test_util23 — wt verify: scratch buffer not leaked on invalid usage path

**File:** `test/suite/test_util23.py`
**Storage mode:** General (skipped for disagg — read-only connections not supported)
**Components under test:** `wt verify`, scratch buffer lifecycle, error handling

## Test Cases

### `test_util23.test_verify_scratch_buffer`
- **What it tests:** Creates a file table; runs `wt -r verify -d dump_offsets <uri>` (read-only connection with dump_offsets option, which is an invalid combination); expects failure with "usage:" error; verifies the error file does NOT contain `'scratch buffer allocated and never discarded'` (ensures no memory/resource leak on the invalid-argument code path).
- **Components:** `util_verify.c`, `verify.c`, `buf.c`
- **Notes:** Skipped for disagg (FIXME-WT-17177). Regression test for a specific bug where the verify scratch buffer was allocated but never freed when the command failed due to bad arguments. No parameterization.

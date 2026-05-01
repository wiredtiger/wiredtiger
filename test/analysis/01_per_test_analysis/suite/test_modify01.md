# test_modify01 — wiredtiger_calc_modify API correctness

**File:** `test/suite/test_modify01.py`
**Storage mode:** General
**Components under test:** `wiredtiger_calc_modify`, `cursor.modify`, partial update encoding

## Test Cases

### `test_modify01.test_modify01`
- **What it tests:** For 1000 random combinations of value size, repetition pattern, number of modifications, and maximum diff size, calls `create_mods` (which internally calls `wiredtiger_calc_modify`) to compute a set of modify descriptors, applies them via `cursor.modify`, and verifies the result equals the expected new value.
- **Components:** `src/cursor/cur_modify.c`, `src/support/modify.c`
- **Notes:** Parameterized by value format:
  - `item` — `value_format='u'` (byte array)
  - `string` — `value_format='S'` (string)

  Uses `random.Random(42)` for reproducibility. Parameters per iteration:
  - `size`: 1000–10000 bytes
  - `repeats`: 1–size (how repetitive the data is)
  - `nmods`: 1–10 modifications per calc_modify call
  - `maxdiff`: 64–size/10 (maximum allowed byte difference)

  Each value is inserted at the chosen key before calling `modify`. Uses a commit timestamp equal to `k+1` for disagg hook compatibility. If `create_mods` returns `None` (edit exceeds `maxdiff`), the test accepts that as valid behavior — though the assertion `assertIsNotNone(mods)` would catch a non-None failure. The test verifies the exact post-modify value matches what `create_mods` computed as `newv`.
